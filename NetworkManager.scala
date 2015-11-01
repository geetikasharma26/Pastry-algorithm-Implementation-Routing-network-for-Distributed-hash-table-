import scala.actors.Actor
import scala.collection.mutable.ArrayBuffer

class NetworkManager(noOfNodes: Int) {
  var base: Int = 4
  var nodeIDLength: Int = 0
  var nodeIDArrayBuffer: ArrayBuffer[String] = new ArrayBuffer

  def getRandomIds(): ArrayBuffer[String] =
    {
      nodeIDLength = getNodeIdLength();
      nodeIDLength = 8
      generateNodeIDs()
      return nodeIDArrayBuffer

    }

  def getBase4Number(number: Int): String =
    {
      var num = number
      var quotient = 5
      var outputNumber = ""
      while (quotient > 3) {
        var remainder = num % 4
        outputNumber += remainder
        num = num / 4
        quotient = num
      }

      return (outputNumber + quotient).reverse
    }

  def generateNodeIDs() = {
    for (i <- 0 to noOfNodes - 1) {
      var nodeID = getNodeID(nodeIDLength)
      while (nodeIDArrayBuffer.contains(nodeID)) {
        nodeID = getNodeID(nodeIDLength)
      }
      nodeIDArrayBuffer += nodeID
    }
  }
  def getNodeID(nodeIDLength: Int): String =
    {
      var rnd = new scala.util.Random
      var range = 0 to 100000
      var randomInt: Int = range(rnd.nextInt(range length))

       //var nodeIDOctal = randomInt.toOctalString
      var nodeIDOctal = getBase4Number(randomInt)
      return wrapNumber(nodeIDOctal)
    }
  def wrapNumber(number: String): String =
    {
      var testNUmber = number
      if (testNUmber == null)
        return null

      if (testNUmber.length() == nodeIDLength) {
        return testNUmber
      } else if (testNUmber.length() < nodeIDLength) {
        while (testNUmber.length() != nodeIDLength) {
          testNUmber = "0" + testNUmber
        }
        return testNUmber
      } else {
        return testNUmber.substring(0, nodeIDLength)
      }
    }

  def getNodeIdLength(): Int =
    {
      var length = 1;
      var maxNumber = base;

      while (maxNumber <= noOfNodes) {
        maxNumber = maxNumber * base
        length += 1
      }

      return length
    }
}