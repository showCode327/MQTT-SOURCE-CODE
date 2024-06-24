import { type Duplex, EventEmitter } from 'stream'
import { Writable } from 'readable-stream'
import net from 'net'
import _debug from 'debug'
import mqttPacket, { IConnectPacket, Packet } from 'mqtt-packet'

interface IClientOptions {
	// 协议类别
	protocol?: 'mqtt'
	// 端口号
	port?: number
	// 主机名
	host?: string
	// 客户端 ID，随机生成的，表明一个客户端连接
	clientId?: string
	// 默认使用 debug，可以由用户自定义，可以传入最简单的 console.log，日志就会被打印出来
	log?: (...args: any[]) => void
	// 保活设置，默认为 60，如果设置为 0，则进行关闭
	keepalive?: number
	// ping 调度的开关，当打开的时候，每次发送消息时会进行 ping 消息的调度
	reschedulePings?: boolean
	// 协议的 ID，默认为 MQTT
	protocolId?: IConnectPacket['protocolId']
	// 协议的版本，默认为 4，【3 表示 3.1】 【4 表示 3.1.1】【5 表示 5.0】
	protocolVersion?: IConnectPacket['protocolVersion']
	// 默认值为 1000 毫秒，表示两次重连的间隔
	reconnectPeriod?: number
	// 30 * 1000 毫秒，即 30 秒，等待收到 CONNACK 的时间
	connectTimeout?: number
	// 清理标识，默认为 true，如果是 false，则会在离线的时候接收 QoS 1 和 2 的消息
	clean?: boolean
	// 默认为 true，如果是 false，则不会进行自动订阅
	resubscribe
	// 控制是否需要写入流时缓存数据
	writeCache?: boolean
}

export type DoneCallback = (error?: Error) => void

export type IStream = Duplex & {
	socket?: any
}

const defaultConnectOptions: IClientOptions = {
	keepalive: 60,
	reschedulePings: true,
	protocolId: 'MQTT',
	protocolVersion: 4,
	reconnectPeriod: 1000,
	connectTimeout: 30 * 1000,
	clean: true,
	resubscribe: true,
	writeCache: true,
}

class Client extends EventEmitter {
	// 连接参数
	public options: IClientOptions

	// 连接流
	public stream: IStream

	// 日志打印，通过参数来控制
	public log: (...args: any[]) => void

	public noop: (error?: any) => void

	constructor(options) {
		super()
		this.options = options || {}

		// 设置默认的参数
		for (const k in defaultConnectOptions) {
			if (typeof this.options[k] === 'undefined') {
				this.options[k] = defaultConnectOptions[k]
			}
		}

		this.log = this.options.log || _debug('MQTT:client')
		// 空函数
		this.noop = this._noop.bind(this)

		// 连接的唯一标识
		const defaultId = () => {
			return `mqttjs_${Math.random().toString(16).substr(2, 8)}`
		}

		// 获取到连接的唯一标识
		this.options.clientId =
			typeof options.clientId === 'string'
				? options.clientId
				: defaultId()

		// 发起连接
		this.connect()
	}

	private createConnectionStream = (opts) => {
		// 连接的端口号
		opts.port = opts.port || 1883
		// 主机名
		opts.hostname = opts.hostname || opts.host || 'localhost'

		const { port } = opts
		const host = opts.hostname

		this.log('端口 %d and 地址 %s', port, host)
		// 进行连接
		return net.createConnection(port, host)
	}

	connect() {
		// 根据参数进行数据包解析的订阅
		const parser = mqttPacket.parser(this.options)
		// 连接流
		this.stream = this.createConnectionStream(this.options)
		// 解析回调
		let completeParse = null

		// 这里是数据从可写流中解析完毕之后执行
		const work = () => {
			const packet = packets.shift()
			this.log('work :: 在队列中获取下一包的数据: ', packet)
			if (packet) {
				this.log('work :: 从队列中获取到一包数据')
				this._handlePacket(packet)
			} else {
				this.log('work :: 队列中没有数据')
			}
		}

		// 实例化一个可写流
		const writable = new Writable()

		// 这里需要解释一下，这里是监听可写流的数据的写入，就是当有数据回传回来的时候，会触发这里的回调，并进行数据的解析
		// 这里还有一个关键是 _write 的参数 done，表示的是解析完毕之后的回调
		writable._write = (buf, _, done) => {
			this.log('writable stream :: 解析 buffer, buffer 内容: ', buf)
			// 在这里进行解析，调用这个函数之后，解析完会触发 parser.on('packet')，得到的就是解析完的数据
			parser.parse(buf)
			// 这里很关键，表示的是解析完毕之后的回调
			completeParse = done
			// 执行指令处理
			work()
		}

		// 将可写流和解析器进行绑定
		this.stream.pipe(writable)

		// 将接收并且解析到的数据包推送到数组中
		const packets = []

		parser.on('packet', (packet) => {
			this.log('parser :: 将数据 push 到数组中.数据内容：%s', packet)
			packets.push(packet)
		})

		// 连接指令
		const connectPacket: IConnectPacket = {
			cmd: 'connect',
			protocolId: this.options.protocolId,
			protocolVersion: this.options.protocolVersion,
			clean: this.options.clean,
			clientId: this.options.clientId,
			keepalive: this.options.keepalive,
		}

		// 发送数据
		this._writePacket(connectPacket)
	}

	private _writePacket(packet: Packet) {
		// 这个是打印的函数，方便我们进行调试
		this.log('_writePacket :: 要发送的数据包：%O', packet)
		this.log('_writePacket :: 发送数据包')

		this.log('_writePacket :: 触发数据发送 `packetsend`')
		this.emit('packetsend', packet)

		// 将数据解析成 Buffer，并写入到流中
		const result = mqttPacket.writeToStream(
			packet,
			this.stream,
			this.options,
		)

		this.log('_writePacket :: 写入数据流结果： %s', result)
	}

	private _handlePacket(packet) {
		// 取出一条命令
		const { cmd } = packet
		this.log('_handlePacket :: 开始处理命令： %s', cmd)

		switch (cmd) {
			case 'connack':
				// 触发 connect 事件，提供外界调用，外界可以使用：client.on('connect', () => {}) 来监听 connect 事件
				this.emit('connect', packet)
				break
			default:
				this.log('_handlePacket :: 未知命令： %s', cmd)
				this._noop()
		}
	}

	private _noop(err?: Error) {
		this.log('noop ::', err)
	}
}

const options = {
	host: 'test.mosquitto.org',
	log: (...args) => console.log(...args),
}

const client = new Client(options)

client.on('connect', () => {
	console.log('连接成功啦')
})
