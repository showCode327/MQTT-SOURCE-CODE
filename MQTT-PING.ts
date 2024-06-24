import { type Duplex, EventEmitter } from 'stream'
import { Writable } from 'readable-stream'
import net from 'net'
import _debug from 'debug'
import mqttPacket, {
	IConnectPacket,
	IPacket,
	IPublishPacket,
	ISubscription,
	Packet,
	QoS,
	UserProperties,
} from 'mqtt-packet'

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

type DoneCallback = (error?: Error) => void

type IStream = Duplex & {
	socket?: any
}

interface ISubscriptionRequest extends IClientSubscribeOptions {
	topic: string
}

interface ISubscriptionGrant
	extends Omit<ISubscriptionRequest, 'qos' | 'properties'> {
	qos: QoS | 128
}

type ClientSubscribeCallback = (
	err: Error | null,
	granted?: ISubscriptionGrant[],
) => void

// 服务质量，为了篇幅和简单起见，我们目前只支持 0
interface IClientSubscribeOptions {
	qos: QoS
}

interface ISubscribePacket extends IPacket {
	cmd: 'subscribe'
	subscriptions: ISubscription[]
	properties?: {
		reasonString?: string
		subscriptionIdentifier?: number
		userProperties?: UserProperties
	}
}

type PacketCallback = (error?: Error, packet?: Packet) => any

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

// 最大的消息标识
const MAX_MESSAGE_ID = 65535

/**
 * 在MQTT协议中，QoS代表"Quality of Service"，即服务质量，用来保证消息能够被成功的传递。在MQTT协议中，QoS有三种等级，如下：
QoS 0 (至多分发一次): 这是最低的QoS等级，协议将只尝试分发消息一次。 如果连接不稳定，可能导致消息丢失。
QoS 1 (至少分发一次): 利用这种等级，MQTT协议保证消息至少分发一次给接收者。但是在某些情况下，可能会导致消息的重复。这种级别常被用于需要确保消息到达但可以容忍重复的情况，例如传感器数据。
QoS 2 (仅分发一次): 这是最高的等级。MQTT协议将保证消息只分发一次给接收者。这个过程涉及到4个步骤的握手，所以会有更多的开销。

在这里，我们为了简单起见，我们默认为 0
 */
const defaultQOS = 0

class Client extends EventEmitter {
	// 连接参数
	public options: IClientOptions

	// 连接流
	public stream: IStream

	// 日志打印，通过参数来控制
	public log: (...args: any[]) => void

	// 空函数
	public noop: (error?: any) => void

	// 下一个消息的标识
	private nextId: number

	// 心跳定时器
	public pingTimer

	//
	public pingResp: boolean

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
		// 心跳定时器
		this.pingTimer = null

		// 连接的唯一标识
		const defaultId = () => {
			return `mqttjs_${Math.random().toString(16).substr(2, 8)}`
		}
		// 初始化 nextId
		this.nextId = Math.max(1, Math.floor(Math.random() * MAX_MESSAGE_ID))

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

		const nextTickWork = () => {
			if (packets.length) {
				process.nextTick(work)
			} else {
				const done = completeParse
				completeParse = null
				done()
			}
		}

		// 这里是数据从可写流中解析完毕之后执行
		const work = () => {
			const packet = packets.shift()
			this.log('work :: 在队列中获取下一包的数据: ', packet)
			if (packet) {
				this.log('work :: 从队列中获取到一包数据')
				this._handlePacket(packet, nextTickWork)
			} else {
				this.log('work :: 队列中没有数据')
				const done = completeParse
				completeParse = null
				if (done) done()
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

		this.log('_writePacket :: 发布数据发送 `packetsend`')
		this.emit('packetsend', packet)

		// 将数据解析成 Buffer，并写入到流中
		const result = mqttPacket.writeToStream(
			packet,
			this.stream,
			this.options,
		)

		this.log('_writePacket :: 写入数据流结果： %s', result)
	}

	private _handlePacket(packet, done) {
		// 取出一条命令
		const { cmd } = packet
		this.log('_handlePacket :: 开始处理命令： %s', cmd)

		switch (cmd) {
			case 'connack':
				// 发布 connect 事件，提供外界调用，外界可以使用：client.on('connect', () => {}) 来监听 connect 事件
				this.emit('connect', packet)
				// 启动心跳
				this._setupPingTimer()
				done()
				break
			case 'suback':
				if (!packet.granted) {
					this.log('suback :: 订阅失败')
					this.emit('error', packet)
				} else {
					this.log('suback :: 订阅成功')
				}
				done()
				break
			case 'publish':
				// eslint-disable-next-line no-case-declarations
				const topic = packet.topic.toString()
				// eslint-disable-next-line no-case-declarations
				const message = packet.payload
				// 发布 message 事件，提供外界调用，外界可以使用：client.on('message', (topic, message) => {}) 来监听 message 事件
				this.emit('message', topic, message as Buffer, packet)
				done()
				break
			case 'pingresp':
				this.pingResp = true
				this.log('pingresp :: 收到 PINGRESP 指令')
				done()
				break
			default:
				// eslint-disable-next-line no-case-declarations
				const msg = `_handlePacket :: 未知命令： ${cmd}`
				this.log(msg)
				this.emit('error', msg)
				done()
		}
	}

	private _noop(err?: Error) {
		this.log('noop ::', err)
	}

	public subscribe(topic: string, callback?: ClientSubscribeCallback) {
		// 避免为了防止用户不传入参数导致报错
		callback = callback || this.noop

		const defaultOpts: Partial<IClientSubscribeOptions> = {
			qos: defaultQOS,
		}

		const subs = [
			{
				topic,
				qos: defaultOpts.qos,
			},
		]

		// 构造订阅指令
		const packet: ISubscribePacket = {
			cmd: 'subscribe',
			subscriptions: subs,
			messageId: this._nextId(),
		}

		// 发送订阅指令
		this._writePacket(packet)
		callback(null, subs)

		return this
	}

	private _nextId(): number {
		const id = this.nextId++

		if (this.nextId === MAX_MESSAGE_ID) {
			this.nextId = 1
		}

		return id
	}

	public publish(topic: string, message: string, callback?: PacketCallback) {
		this.log('publish ::   向主题 `%s` 发送消息 `%s`', topic, message)

		const packet: IPublishPacket = {
			cmd: 'publish',
			topic, // 订阅的主题
			payload: message, // 要发送的消息
			qos: defaultQOS, // 服务质量，默认为 0
			dup: false, // 重发标识，默认为 false
			retain: false, // 保留标识，默认为 false
		}

		// 发送消息
		this._writePacket(packet)

		return this
	}

	// 做心跳检查
	private _checkPing() {
		this.log('_checkPing :: 检查心跳中 ...')
		if (this.pingResp) {
			this.log(
				'_checkPing :: 心跳已经收到 `pingresp` 指令，可以发送 `pingreq`',
			)
			this.pingResp = false
			this._writePacket({ cmd: 'pingreq' })
		} else {
			this.emit('error', new Error('Keepalive timeout'))
			this.pingTimer = null
		}
	}

	// 清空定时器，走下一个周期
	private _cleanPingTimer = () => {
		clearTimeout(this.pingTimer)
		this.pingTimer = null
		this._reschedule()
	}

	private _reschedule() {
		clearTimeout(this.pingTimer)
		// 设置定时器，检查心跳
		this.pingTimer = setTimeout(() => {
			// 启动检查
			this._checkPing()
			if (this.pingTimer) {
				// 准备下一次检查
				this._cleanPingTimer()
			}
		}, this.options.keepalive * 1000)
	}

	// 设置定时器和心跳检查
	private _setupPingTimer() {
		this.log(
			'_setupPingTimer :: 保活设置 %d (seconds)',
			this.options.keepalive,
		)
		if (!this.pingTimer && this.options.keepalive) {
			// 默认设置标识为 true
			this.pingResp = true
			this._reschedule()
		}
	}
}

const options = {
	host: 'test.mosquitto.org',
	log: (...args) => console.log(...args),
}

const client = new Client(options)
const TOPIC = 'presence'

client.on('connect', () => {
	console.log('连接成功啦')
	client.subscribe(TOPIC, (err) => {
		if (!err) {
			console.log(`订阅主题：${TOPIC} 成功`)
			client.publish('presence', 'Hello mqtt')
		}
	})
})

client.on('message', (topic, message) => {
	console.log(`接收到服务端的消息：${message.toString()}`)
})
