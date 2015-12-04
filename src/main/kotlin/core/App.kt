package core

import co.paralleluniverse.actors.ActorRef
import co.paralleluniverse.actors.ActorRegistry
import co.paralleluniverse.fibers.Suspendable
import co.paralleluniverse.kotlin.Actor
import co.paralleluniverse.kotlin.register
import co.paralleluniverse.kotlin.spawn
import co.paralleluniverse.strands.Strand
import java.util.*
import co.paralleluniverse.strands.Timeout as TimeOut
import java.util.concurrent.TimeUnit

// NOTE: implementation contains some tweaks:
// * T is period of time between events "last PONG received" and "node need to send PING again"
// * After receiving FINETHANKS actor wait 2 * T milliseconds instead of T for IAMTHEKING. This is because the highest alive actor will wait T for FINETHANKS from dead king)

enum class MessageType {
    PING,
    PONG,
    ALIVE,
    FINETHANKS,
    IAMTHEKING,
    KILL
}

enum class NodeState {
    KING,
    IDLE,
    WAITING_FOR_PONG,
    WAITING_FOR_FINE,
    WAITING_FOR_KING,
}

data class LocalData(
        val id: Int,
        val state: NodeState,
        val nextActionTime: Long = 0,
        val kingId: Int? = null,
        override val shouldRun: Boolean = true) : IActorData

data class Message(val type: MessageType, val from: Int)

class Node(val id: Int, val nodesCount: Int, val timeout: Long, val sys: IActorSystem) : IActor<LocalData> {
    @Suspendable override fun init():LocalData {
        sys.log(id, "started")
        return LocalData(id, NodeState.WAITING_FOR_PONG)
    }

    @Suspendable override fun tick(data:LocalData):LocalData {
        val newData = if (sys.now() >= data.nextActionTime) {
            checkState(data)
        } else data
        return receiveMessage(newData)
    }

    @Suspendable fun checkState(data: LocalData): LocalData {
        return when (data.state) {
            NodeState.IDLE -> {
                if (data.kingId == null) {
                    throw IllegalStateException("Actor ${data.id} in IDLE state with kindId=null")
                }
                sys.send(data.kingId, Message(MessageType.PING, data.id))
                data.copy(state = NodeState.WAITING_FOR_PONG, nextActionTime = sys.now() + 4 * timeout)
            }
            NodeState.WAITING_FOR_PONG -> {
                if (data.kingId != null) {
                    sys.log(data.id, "old king is dead")
                }
                startElection(data)
            }
            NodeState.WAITING_FOR_FINE -> {
                becomeKing(data)
            }
            NodeState.WAITING_FOR_KING -> {
                startElection(data)
            }
            NodeState.KING -> {
                throw IllegalStateException("Actor ${data.id} in KING state need to action")
            }
        }
    }

    @Suspendable fun receiveMessage(data: LocalData): LocalData {
        val msg = sys.receive(id, data.nextActionTime - sys.now())
        return when (msg) {
            is Message -> {
                sys.log(data.id, "received ${msg.type} from ${msg.from}")
                when (msg.type) {
                    MessageType.PING -> {
                        sys.send(msg.from, Message(MessageType.PONG, data.id))
                        data
                    }
                    MessageType.PONG -> {
                        if (msg.from == data.kingId) {
                            data.copy(state = NodeState.IDLE, nextActionTime = sys.now() + timeout)
                        } else data
                    }
                    MessageType.ALIVE -> {
                        sys.send(msg.from, Message(MessageType.FINETHANKS, data.id))
                        startElection(data)
                    }
                    MessageType.FINETHANKS -> {
                        if (data.state == NodeState.WAITING_FOR_FINE) {
                            data.copy(state = NodeState.WAITING_FOR_KING, nextActionTime = sys.now() + 2 * timeout)
                        } else data
                    }
                    MessageType.IAMTHEKING -> {
                        data.copy(state = NodeState.IDLE, nextActionTime = sys.now() + timeout, kingId = msg.from)
                    }
                    MessageType.KILL -> {
                        data.copy(shouldRun = false)
                    }
                }
            }
            null -> {
                //do nothing - it's a timeout
                data
            }
            else -> {
                sys.log(data.id, "got something strange: " + msg)
                data
            }
        }
    }

    @Suspendable fun becomeKing(data: LocalData): LocalData {
        ((data.id + 1) until nodesCount).forEach { sys.send(it, Message(MessageType.IAMTHEKING, data.id)) }
        return data.copy(state = NodeState.KING, nextActionTime = Long.MAX_VALUE, kingId = data.id)
    }

    @Suspendable fun startElection(data: LocalData): LocalData {
        return if (data.id == 0) {
            becomeKing(data)
        } else {
            (0 until data.id).forEach { sys.send(it, Message(MessageType.ALIVE, data.id)) }
            data.copy(state = NodeState.WAITING_FOR_FINE, nextActionTime = sys.now() + timeout, kingId = null)
        }
    }

}

fun startNodes(actorSystem: IActorSystem, nodesCount: Int, timeout: Long) {
    (0 until nodesCount).forEach { actorSystem.spawn(it, Node(it, nodesCount, timeout, actorSystem)) }
}

class QuasarActorWrapper<T:IActorData>(val actor: IActor<T>) : Actor () {
    @Suspendable override fun doRun(): Any? {
        val data = actor.init()
        loop(data)
        return null
    }

    @Suspendable tailrec fun loop(data: T) {
        val newData = actor.tick(data)
        if (newData.shouldRun) {
            loop(newData)
        }
    }
}

class QuasarActorSystem : IActorSystem {
    val actorsMap = HashMap<Int, Pair<ActorRef<Any?>, Actor>>()

    @Suspendable fun getActor(id:Int) = actorsMap[id] ?: throw IllegalArgumentException("Actor with id $id not found")

    @Suspendable override fun <T:IActorData> spawn(id: Int, actor: IActor<T>) {
        val wrapper = QuasarActorWrapper(actor)
        val ref = spawn(wrapper)
        actorsMap[id] = Pair(ref, wrapper)
    }

    @Suspendable override fun receive(id: Int, timeout: Long): Any? {
        val actor = getActor(id)
        val second = actor.second
        return second.receive(timeout, TimeUnit.MILLISECONDS)
    }

    @Suspendable override fun send(to: Int, msg: Message) {
        getActor(to).first.send(msg)
    }

    @Suspendable override fun now() = System.currentTimeMillis()

    @Suspendable override fun sleep(millis:Long) {
        Strand.sleep(millis)
    }

    val t0 = now() //for debuging purposes
    @Suspendable override fun log(id: Int?, text: String) {
        println((now() - t0).toString() + " " + (id ?: "SYSTEM") + " " + text)
    }
}

fun main(args: Array<String>) {
    val sys = QuasarActorSystem()
    startNodes(sys, nodesCount = 4, timeout = 1000)
    sys.log(null, "waiting for kill")
    sys.sleep(5000)
    sys.send(0, Message(MessageType.KILL, 0))
    sys.log(null, "sending kill to 0")
    sys.sleep(10000)
}

