import core.*
import core.MessageType.*
import org.junit.Test
import java.util.*
import kotlin.test.assertEquals

public class ActorTest {
    @Test public fun generalScenario() {
        var sys:TestActorSystem = TestActorSystem()
        startNodes(sys, nodesCount = 5, timeout = 500)

        (0 until 100).forEach {
            sys.sleep(50)
            sys.tickAll()
        }
        assertEquals(0, (sys.getDataOf(0) as LocalData).kingId)
        assertEquals(0, (sys.getDataOf(1) as LocalData).kingId)
        assertEquals(0, (sys.getDataOf(2) as LocalData).kingId)

        sys.send(0, Message(KILL, 0))
        sys.log(null, "sending kill to 0")

        (0 until 100).forEach {
            sys.sleep(50)
            sys.tickAll()
        }
        assertEquals(1, (sys.getDataOf(1) as LocalData).kingId)
        assertEquals(1, (sys.getDataOf(2) as LocalData).kingId)
    }

    @Test public fun stepByStep() {
        val sys = TestActorSystem()

        fun msgCount(id:Int):Int = sys.getQueueOf(id).size
        fun firstMsgOf(id:Int):Message = sys.getQueueOf(id)[0] as Message
        fun secondMsgOf(id:Int):Message = sys.getQueueOf(id)[1] as Message

        val timeout = 1000L
        startNodes(sys, nodesCount = 3, timeout = timeout)

        assertEquals(0, msgCount(0))
        assertEquals(0, msgCount(1))
        assertEquals(0, msgCount(2))

        sys.tick(0)
        assertEquals(0, msgCount(0))
        assertEquals(Message(IAMTHEKING, 0), firstMsgOf(1))
        assertEquals(Message(IAMTHEKING, 0), firstMsgOf(2))

        sys.sleep(10)
        sys.tick(1)
        assertEquals(Message(ALIVE, 1), sys.getQueueOf(0).first())
        assertEquals(0, msgCount(1))

        sys.sleep(10)
        sys.tick(0)
        assertEquals(Message(FINETHANKS, 0), firstMsgOf(1))
        assertEquals(Message(IAMTHEKING, 0), secondMsgOf(1))
        assertEquals(Message(IAMTHEKING, 0), secondMsgOf(2))

        sys.sleep(10)
        sys.tick(1)
        sys.sleep(10)
        sys.tick(1)

        sys.sleep(10)
        sys.tick(2)
        assertEquals(Message(ALIVE, 2), firstMsgOf(0))
        assertEquals(Message(ALIVE, 2), firstMsgOf(1))

        sys.sleep(10)
        sys.tick(2)
        assertEquals(0, msgCount(2))

        sys.sleep(10)
        sys.tick(1)
        assertEquals(Message(FINETHANKS, 1), firstMsgOf(2))
        assertEquals(Message(ALIVE, 1), secondMsgOf(0))

        sys.sleep(10)
        sys.tick(2)
        sys.tick(1)
        assertEquals(0, msgCount(1))
        assertEquals(0, msgCount(2))
        assertEquals(2, msgCount(0))

        sys.sleep(10)
        sys.tick(2)
        sys.tick(1)
        assertEquals(0, msgCount(1))
        assertEquals(0, msgCount(2))

        sys.sleep(10)
        sys.tick(0)
        assertEquals(Message(FINETHANKS, 0), firstMsgOf(2))
        assertEquals(Message(IAMTHEKING, 0), secondMsgOf(2))
        assertEquals(Message(IAMTHEKING, 0), firstMsgOf(1))

        sys.sleep(10)
        sys.tick(1)
        sys.tick(2)

        sys.sleep(10)
        sys.tick(1)
        sys.tick(2)

        sys.sleep(10)
        sys.tick(0)
        assertEquals(Message(FINETHANKS, 0), firstMsgOf(1))
        assertEquals(Message(IAMTHEKING, 0), secondMsgOf(1))
        assertEquals(Message(IAMTHEKING, 0), firstMsgOf(2))

        sys.sleep(10)
        sys.tick(1)
        sys.tick(2)
        val king2 = sys.now()

        sys.sleep(10)
        sys.tick(1)
        val king1 = sys.now()

        assertEquals(0, msgCount(0))
        assertEquals(0, msgCount(1))
        assertEquals(0, msgCount(2))

        //0 is king, 1 and 2 knows this
        sys.sleep(10)
        sys.tickAll()

        assertEquals(0, msgCount(0))
        assertEquals(0, msgCount(1))
        assertEquals(0, msgCount(2))

        sys.setTime(king2 + timeout)

        sys.tickAll()
        assertEquals(0, msgCount(1))
        assertEquals(0, msgCount(2))
        assertEquals(Message(PING, 2), firstMsgOf(0))

        sys.sleep(10)
        sys.tick(0)
        assertEquals(Message(PONG, 0), firstMsgOf(2))
        assertEquals(0, msgCount(1))

        sys.setTime(king1 + timeout + 100)
        sys.tick(1)
        assertEquals(Message(PING, 1), firstMsgOf(0))

        sys.sleep(timeout * 2)
        sys.tick(0)
        assertEquals(Message(PONG, 0), firstMsgOf(1))
    }

}

class TestActor<Data : IActorData>(val inner: IActor<Data>) : IActor<Data> by inner {
    val queue = LinkedList<Any?>()
}

class TestActorSystem : IActorSystem {
    private var currentTime: Long = 0
    val actorsMap = HashMap<Int, TestActor<out IActorData>>()
    val actorsData = HashMap<Int, IActorData>()

    fun getActor(id: Int) = actorsMap[id] ?: throw IllegalArgumentException("Actor with id $id not found")

    override fun <Data : IActorData> spawn(id: Int, actor: IActor<Data>) {
        actorsMap[id] = TestActor(actor)
        actorsData[id] = actor.init()
    }

    override fun receive(id: Int, timeout: Long): Any? {
        return getActor(id).queue.pollFirst()
    }

    override fun send(to: Int, msg: Message) {
        log(msg.from, "send ${msg.type} to $to")
        getActor(to).queue.addLast(msg)
    }

    override fun now() = currentTime

    override fun sleep(millis: Long) {
        currentTime += millis
    }

    fun setTime(millis: Long) {
        if (currentTime > millis) {
            throw IllegalArgumentException("Trying to jump back in time. Current: $currentTime new: $millis")
        }
        currentTime = millis
    }

    var t0 = now() //for debugging purposes
    override fun log(id: Int?, text: String) {
        val s = (now() - t0).toString() + " " + (id ?: "SYSTEM") + " " + text
        println(s)
    }

    fun tickAll() {
        actorsMap.keys.forEach { tick(it) }
    }

    fun tick(id:Int) {
        val data = actorsData[id]!!
        if (data.shouldRun) {
            val actor = actorsMap[id]!! as IActor<IActorData>
            val newData = actor.tick(data)
            actorsData[id] = newData
        }
    }

    fun getQueueOf(id:Int) = actorsMap[id]!!.queue

    fun getDataOf(id:Int) = actorsData[id]!!
}
