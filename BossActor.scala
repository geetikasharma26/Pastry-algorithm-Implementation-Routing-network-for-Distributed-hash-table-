import scala.actors.Actor
import scala.collection.mutable.ArrayBuffer
import java.util.Random
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashMap

class BossActor(noOfNodes: Int, noOfRequests: Int) extends Actor {
  var newNodeList: ArrayBuffer[WorkerActor] = new ArrayBuffer();
  var pastryNodeList: ArrayBuffer[WorkerActor] = new ArrayBuffer();
  var idAlottedList: ArrayBuffer[Int] = new ArrayBuffer[Int]()
  var nodeIdAndRef: HashMap[String, WorkerActor] = new HashMap[String, WorkerActor]()

  def act() {
    createWorkers();
    updateALLNodes();
    createPastry();
    Thread.sleep(10 * 1000)
    /*printPastryNodes()*/
    getNodeConnections()
    routeMsg()

    var totalConnections: Int = 0
    var totalAck: Int = 0
    var totalHops: Int = 0
    loop {
      react {
        case ("NODECONNECTIONS", noOfConnections: Int) =>
          totalConnections += noOfConnections

        case (count: Int, noOfHopes: Int) =>
          totalAck += count
          totalHops += noOfHopes
          if (totalAck == noOfNodes * noOfRequests) {
            println("TOTAL NUMBER OF HOPS : " + totalHops)
            //println("TOTAL NUMBER OF Connections : " + totalConnections)
            println("Average number of hops : " + Math.ceil(((totalHops*1.0 )/ (noOfNodes*noOfRequests))))
            System.exit(0)
          }
      }
    }

  }

  def getNodeConnections(): Unit =
    {
      for (i <- 0 to newNodeList.length - 1) {
        newNodeList(i) ! "NODECONNECTIONS"
      }
    }

  def updateALLNodes(): Unit =
    {
      for (i <- 0 to newNodeList.length - 1) {
        newNodeList(i) ! nodeIdAndRef
        newNodeList(i) ! this
      }
    }

  def routeMsg(): Unit =
    {
      for (i <- 0 to newNodeList.length - 1) {
        var messageKeys: ArrayBuffer[String] = (new NetworkManager(noOfRequests)).getRandomIds()
        var selectNode = newNodeList(i)

        for (j <- 0 to noOfRequests - 1) {
          selectNode ! ("ROUTE", messageKeys(j), "Hello World", 0)
          Thread.sleep(1 * 1000)
        }
      }
    }

  /*def printPastryNodes() : Unit=
{
		for (iterator <- 0 to newNodeList.length -1 )
		{
			println("-----------------------------------------NODE ID : "+ newNodeList(iterator).myID + "------------------------------------------")
			newNodeList(iterator).showRoutingTable()
			println("-----------------------------------------------------------------------------------------------------------")
			newNodeList(iterator).showLeafset()
			println("-----------------------------------------------------------------------------------------------------------")
			println("-----------------------------------------------------------------------------------------------------------")
		}
}*/

  def createPastry() {
    var pastryNode: WorkerActor = null
    var newNode: WorkerActor = null
    var len = newNodeList.length
    for (i <- 0 to len - 1) {

      pastryNode = selectRandomPastryNode()
      newNode = newNodeList(i)
      if (pastryNode == null) //=>Network is empty
      {
        newNode ! "start"
      } else {
        pastryNode ! (newNode, "JOIN", 0)

      }

       pastryNodeList += newNode
    }

  }

  def createWorkers() {
    var nDigits = noOfNodes.toString().length()
    var rangeStart = Math.pow(10, nDigits).toInt
    val rnd = new scala.util.Random
    val range = rangeStart to (rangeStart + noOfNodes)
    var randomId: String = "";
    var randomIdBuffer: ArrayBuffer[String] = (new NetworkManager(noOfNodes)).getRandomIds()
     for (i <- 0 to noOfNodes - 1) {
      randomId = randomIdBuffer(i)
      var worker: WorkerActor = new WorkerActor(randomId, 8, 4);
      worker.start()
      newNodeList += worker;
      nodeIdAndRef += randomId -> worker
      pastryNodeList += worker
    }
  }

  def selectRandomNewNode(): WorkerActor =
    {
      if (newNodeList.length != 0) {
        val rnd = new scala.util.Random
        val range = 0 to newNodeList.length - 1
        var randomId: Int = range(rnd.nextInt(range length));
        var randomNode = newNodeList(randomId)
        return randomNode
      }
      return null
    }

  def selectRandomPastryNode(): WorkerActor =
    {
      if (pastryNodeList.length != 0) {
        val rnd = new scala.util.Random
        val range = 0 to pastryNodeList.length - 1
        var randomId: Int = range(rnd.nextInt(range length));
        return pastryNodeList(randomId);
      }
      return null
    }

}