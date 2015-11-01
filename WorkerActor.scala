import scala.collection.mutable.ArrayBuffer
import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable.HashMap
import com.sun.org.apache.xml.internal.serializer.ToStream
import com.sun.rowset.internal.Row
import java.sql.RowId

class WorkerActor(id: String, noOfRows: Int, noOfEntries: Int) extends Actor {

  var myID = id
  var routingTable: Array[Array[String]] = Array.ofDim[String](noOfRows, noOfEntries); //new Array[Array[String]](noOfRows, noOfEntries); // every routing table has multiple rows. Each row has multiple node entries which is an object of NodeRefId 
  var leafSet: Array[Array[String]] = Array.ofDim[String](2,4)	//new Array[Array[String]](2, 4);
  var nodeMap: HashMap[String, WorkerActor] = new HashMap[String, WorkerActor]()
  var myBoss: BossActor = null
  var myConnectionMap: HashMap[String, Int] = new HashMap[String, Int]()
  var JOIN = "JOIN"
  var MYSELF = "MYSELF"
  var ROUTE = "ROUTE"

  def act() {
    initializeTables()
    loop {
      react {

        case ("NODECONNECTIONS") =>
          var myconnections = getMyConnections()
          myBoss ! ("NODECONNECTIONS", myconnections)

        case (nodeIdAndRef: HashMap[String, WorkerActor]) =>
          nodeMap = nodeIdAndRef

        case (node: BossActor) =>
          myBoss = node

        case ("ROUTE", messageKey: String, message: String, hopCount: Int) =>
          routeMessage(messageKey, message, hopCount)

        case "ping" =>
          sender ! "success"

        case "start" =>

        case (newNode: WorkerActor, "JOIN", mCount: Int) =>
          performJoin(newNode, mCount)

        case (rowIndex: Int, row: Array[String], "updateRow") =>
          updateRoutingTable(rowIndex, row)

        case (node: WorkerActor, "addEntry") =>
          updateRoutingTable(node.myID)
          adjustLeafset(node.myID)

        case (node: WorkerActor, "addEntry", newLeafSet: Array[Array[String]], newRoutingTable: Array[Array[String]]) =>
          updateMyLeafSet(newLeafSet)
          updateRoutingTable(node.myID)
          adjustLeafset(node.myID)
          updateMyRoutingTable(newRoutingTable)

        case (newLeafSet: Array[Array[String]], "updateLeafSet") =>
          updateMyLeafSet(newLeafSet)
          adjustLeafset(sender.asInstanceOf[WorkerActor].myID)

        case ("sendUpdates") =>
          sendRoutingUpdates()

      }
    }
  }

  def updateMyRoutingTable(newRoutingTable: Array[Array[String]]): Unit =
    {
      for (i <- 0 to routingTable.length - 1) {
        for (j <- 0 to routingTable(i).length - 1) {
          if (routingTable(i)(j) != null && !routingTable(i)(j).equalsIgnoreCase(this.myID)) {
            updateRoutingTable(routingTable(i)(j))
          }
        }
      }
    }

  def getMyConnections(): Int =
    {
      getLeafSetConnection()
      getRoutingTableConnection()
      return myConnectionMap.size
    }

  def getRoutingTableConnection(): Unit =
    {
      for (i <- 0 to routingTable.length - 1) {
        for (j <- 0 to routingTable(i).length - 1) {
          if (routingTable(i)(j) != null && !myConnectionMap.contains(routingTable(i)(j))) {
            myConnectionMap += routingTable(i)(j) -> 1
          }
        }
      }
    }

  def getLeafSetConnection(): Unit =
    {
      for (i <- 0 to leafSet.length - 1) {
        for (j <- 0 to leafSet(i).length - 1) {
          if (leafSet(i)(j) != null && !myConnectionMap.contains(leafSet(i)(j))) {
            myConnectionMap += leafSet(i)(j) -> 1
          }
        }
      }
    }

  def routeMessage(messageKey: String, message: String, hopCount: Int): Unit =
    {
      var communicationType = mybestMatchNode(messageKey)
      var msg = message + " - > [" + myID + "]"
      var bestNodeVarification = verifyBestMatch(communicationType._1, messageKey)
      if (bestNodeVarification) {
        if (communicationType._2.equalsIgnoreCase(JOIN)) {
          nodeMap(communicationType._1) ! (ROUTE, messageKey, msg, (hopCount + 1))
        } else if (communicationType._2.equalsIgnoreCase(MYSELF)) {
          println("MyID : " + myID + " , Message Key : " + messageKey + " , message received : " + msg + ", hopCount : " + hopCount + 1)
          myBoss ! (1, hopCount + 1)
        }
      } else {
        println("MyID(FOUND MYSELF AS THE BEST NODE) : " + myID + " , Message Key : " + messageKey + " , message received : " + msg + ", hopCount : " + (hopCount + 1))
        myBoss ! (1, (hopCount + 1))
      }

    }

  def getNodeToBeContacted(failedNodeID: String, rowNo: Int): String =
    {
      var row = routingTable(rowNo)
      var newNodeID = getNode(row, failedNodeID, rowNo)
      if (newNodeID != null && !newNodeID.equals(this.myID)) {
        return newNodeID
      } else {
        if ((rowNo + 1) < routingTable.length) {
          getNodeToBeContacted(failedNodeID, rowNo + 1)
        } else {
          return null
        }

      }
    }

  def getNodeIDfromRemoteNode(contactNodeID: String, rowNo: Int, columnNo: Int): String =
    {
      var remoteNode = nodeMap(contactNodeID)
      return remoteNode.routingTable(rowNo)(columnNo)
    }

  def getNode(row: Array[String], failedNodeID: String, rowNo: Int): String =
    {
      for (i <- 0 to row.length - 1) {
        if (row(i) != null && (row(i) != failedNodeID)) {

          var finalNodeID = getNodeIDfromRemoteNode(row(i), rowNo, i)
          if (finalNodeID != null) {
            return finalNodeID
          }
        }
      }

      return null
    }

  def removeFailedNodeEntry(nodeID: String, rowIndex: Int): Unit =
    {
      var rowNo: Int = rowIndex
      var colIndex: Int = 0

      if (rowIndex == 0) {
        colIndex = 0
      } else {
        colIndex = rowIndex - 1
      }
      var columnNo: Int = nodeID.charAt(colIndex).toString().toInt

      routingTable(rowNo)(columnNo) = null

    }

  def updateRoutingTableForFailure(failedNodeID: String): Boolean =
    {

      var myMatchCount = matchPrefixes(myID, failedNodeID)
      removeFailedNodeEntry(failedNodeID, myMatchCount)

      var contactNode = getNodeToBeContacted(failedNodeID, myMatchCount)
      if (contactNode == null) {
        return false
      } else {
        var rowNo: Int = myMatchCount
        var colIndex: Int = 0

        if (rowNo == 0) {
          colIndex = 0
        } else {
          colIndex = rowNo - 1
        }

        var columnNo: Int = failedNodeID.charAt(colIndex).toString().toInt

        routingTable(rowNo)(columnNo) = contactNode
        return true
      }
    }

  def getNodeToRequestForLeafSetFromLeftArray(failedNode: String): String =
    {
      var leafArray = leafSet(0)
      for (i <- 0 to (leafArray.length - 1)) {
        if (leafArray(i) != null && leafArray(i) != failedNode) {
          return leafArray(i)
        }
      }

      return null
    }

  def getNodeToRequestForLeafSetFromRightArray(failedNode: String): String =
    {
      var leafArray = leafSet(1)
      for (i <- 0 to leafArray.length - 1) {
        if (leafArray(i) != null && leafArray(i) != failedNode) {
          return leafArray(i)
        }
      }

      return null
    }
  def getNodeToRequestForLeafSet(failedNode: String): String =
    {
      var nearestNode: String = null
      if (failedNode < myID) {
        nearestNode = getNodeToRequestForLeafSetFromLeftArray(failedNode)

        if (nearestNode == null)
          nearestNode = getNodeToRequestForLeafSetFromRightArray(failedNode)
      } else {
        nearestNode = getNodeToRequestForLeafSetFromRightArray(failedNode)

        if (nearestNode == null)
          nearestNode = getNodeToRequestForLeafSetFromLeftArray(failedNode)
      }

      return nearestNode
    }

  def getLeafSet(nodeID: String): Array[Array[String]] =
    {
      return nodeMap(nodeID).leafSet
    }

  def updateMyLeafSet(newLeafSet: Array[Array[String]]): Unit =
    {
      for (i <- 0 to 1) {
        for (j <- 0 to newLeafSet(i).length - 1) {
          if (newLeafSet(i)(j) != null && !newLeafSet(i)(j).equalsIgnoreCase(this.myID)) {
            adjustLeafset(newLeafSet(i)(j))
          }
        }
      }
    }

  def initializeTables() =
    {
      for (i <- 0 to routingTable.length - 1) {
        for (j <- 0 to routingTable(i).length - 1) {
          routingTable(i)(j) = null
        }
      }

      for (i <- 0 to leafSet.length - 1) {
        for (j <- 0 to leafSet(i).length - 1) {
          leafSet(i)(j) = null
        }
      }

    }

  // method sending self details to all the entries in the routing table
  // sending self details to all the nodes in the routing table so that they can update their tables
  def sendRoutingUpdates() =
    {
      /** sending updates to leafset entries */
      for (i <- 0 to leafSet.length - 1) {
        for (j <- 0 to leafSet(i).length - 1) {
          if (leafSet(i)(j) != null) {
            var selectNode = nodeMap(leafSet(i)(j))
            selectNode ! (this, "addEntry", this.leafSet, this.routingTable)
            //selectNode ! (this, "addEntry")
          }
        }
      }

      /** sending to entries in routing table*/
      for (i <- 0 to routingTable.length - 1) {
        for (j <- 0 to routingTable(i).length - 1) {
          if (routingTable(i)(j) != null) {
            var selectNode = nodeMap(routingTable(i)(j))
            selectNode ! (this, "addEntry", this.leafSet, this.routingTable)

          }
        }
      }
    }

  //method updating self routing table with the entry received as a part of msg
  def updateRoutingTable(newNodeID: String) =
    {
      var myMatchCount = matchPrefixes(myID, newNodeID)
      var rowIndex = myMatchCount
      if (rowIndex == myID.length())
        rowIndex = rowIndex - 1
      var colIndex: Int = 0

      if (rowIndex == 0) {
        colIndex = 0
      } else {
        colIndex = rowIndex - 1
      }
      var columnNo: Int = newNodeID.charAt(colIndex).toString().toInt
      routingTable(rowIndex)(columnNo) = newNodeID

    }

  //method updating self routing table with the ROW received as a part of msg
  def updateRoutingTable(rowIndex: Int, row: Array[String]) =
    {
      routingTable(rowIndex) = row;
    }

  //Adding a node to the network and finding its appropriate position
  def performJoin(newNode: WorkerActor, mCount: Int) =
    {
      var myMatchCount = matchPrefixes(myID.toString(), newNode.myID.toString())
      var rowIndex = myMatchCount
      var communicationType = mybestMatchNode(newNode.myID)
      var bestNodeVarification = verifyBestMatch(communicationType._1, myID)

      if (bestNodeVarification) {
        if (communicationType._2.equalsIgnoreCase(JOIN)) {
          sendRowsForUpdate(mCount, myMatchCount, newNode)
          nodeMap(communicationType._1) ! (newNode, JOIN, myMatchCount)
        } else if (communicationType._2.equalsIgnoreCase(MYSELF)) {
          if (myMatchCount == this.myID.length()) {
            myMatchCount -= 1
          }
          sendRowsForUpdate(mCount, myMatchCount, newNode)
          newNode ! (leafSet, "updateLeafSet")
          newNode ! (this, "addEntry")
          newNode ! "sendUpdates"
        }
      } else {
        if (myMatchCount == this.myID.length()) {
          myMatchCount -= 1
        }
        sendRowsForUpdate(mCount, myMatchCount, newNode)
        newNode ! (leafSet, "updateLeafSet")
        newNode ! (this, "addEntry")
        newNode ! "sendUpdates"
      }
    }

  def verifyBestMatch(bestMatchNodeId: String, msgKeyNodeId: String): Boolean = //true => bestMAtch is best else(false) currrentNode is best
    {
      var bestDiff = Math.abs(bestMatchNodeId.toInt - msgKeyNodeId.toInt)
      var myDiff = Math.abs(myID.toInt - msgKeyNodeId.toInt)
      if (bestDiff < myDiff) {
        return true
      } else {
        return false
      }
    }

  def mybestMatchNode(newNodeId: String): (String, String, Int) =
    {
      var myMatchCount = matchPrefixes(myID.toString(), newNodeId.toString())

      if (myMatchCount == myID.length()) {
        return (this.myID, MYSELF, 0)
      }
      var bestMatchNode = checkBestNodeInLeafSet(newNodeId)
      if (bestMatchNode == this.myID) {
        //println("MyID : " + this.myID + ", Selected MYSELF as the best node after checking in the leaf sets")
        return (this.myID, MYSELF, 0)
      }

      if (bestMatchNode != null) {
        return (bestMatchNode, JOIN, 1)
      } else {
        bestMatchNode = getFwdNodefromRoutingTable(newNodeId, myMatchCount)

        if (bestMatchNode == null) {
          bestMatchNode = checkBestNodeInLeafSet(newNodeId)
          if (bestMatchNode == this.myID) {
            return (this.myID, MYSELF, 0)
          }

          if (bestMatchNode != null) {
            return (bestMatchNode, JOIN, 1)
          } else {
            return (this.myID, MYSELF, 0)
          }

        } else {
          return (bestMatchNode, JOIN, 2)
        }
      }
    }

  def checkBestNodeInLeafSet(newNodeID: String): String =
    {
      var leftSmallest = getSmallestIDFromLeafSet()
      var rightLargest = getLargestIDFromLeafSet()

      if (leftSmallest == null && rightLargest == null) {
        return null
      }

      if ((newNodeID >= leftSmallest) && (newNodeID <= rightLargest)) {
        return bestMatchingNodeFromLeafSet(newNodeID)
      }

      return null
    }

  def bestMatchingNodeFromLeafSet(newNodeID: String): String =
    {
      var bestMatch: String = myID

      var minDiff = Math.abs(newNodeID.toInt - myID.toInt)
      var newDiff = 0

      for (i <- 0 to leafSet.length - 1) {
        for (j <- 0 to leafSet(i).length - 1) {
          if (leafSet(i)(j) != null) {
            newDiff = Math.abs(leafSet(i)(j).toInt - newNodeID.toInt)
            if (newDiff < minDiff) {
              minDiff = newDiff
              bestMatch = leafSet(i)(j)
            }
          }
        }
      }

      return bestMatch
    }

  def getLargestIDFromLeafSet(): String =
    {
      var largestID: String = null
      for (i <- 0 to leafSet.length - 1) {
        for (j <- 0 to leafSet(i).length - 1) {
          if (leafSet(i)(j) != null) {

            if (largestID == null) {
              largestID = leafSet(i)(j)
            }

            if (leafSet(i)(j) > largestID) {
              largestID = leafSet(i)(j)
            }
          }
        }
      }

      return largestID
    }

  def getSmallestIDFromLeafSet(): String =
    {
      var smallestID: String = null
      for (i <- 0 to leafSet.length - 1) {
        for (j <- 0 to leafSet(i).length - 1) {
          if (leafSet(i)(j) != null) {
            if (smallestID == null) {
              smallestID = leafSet(i)(j)
            }

            if (leafSet(i)(j) < smallestID) {
              smallestID = leafSet(i)(j)
            }
          }
        }
      }

      return smallestID
    }

  def sendRowsForUpdate(start: Int, end: Int, newNode: WorkerActor) {
    for (i <- start to end) {
      var copiedRow = new Array[String](noOfEntries)
      routingTable(i).copyToArray(copiedRow)
      newNode ! (i, copiedRow, "updateRow")
    }
  }

  //method printing routing table of the node
  def showRoutingTable() {
    var route: String = ""
    route += " Node " + myID + "'s Routing Table:\n"
    for (i <- 0 to routingTable.length - 1) {
      route = route + "row " + i + " (no of columns=" + (routingTable(i).length - 1) + "):"
      for (j <- 0 to routingTable(i).length - 1) {
        if (routingTable(i)(j) != null)
          route += " worker not null at " + j + " and ID " + routingTable(i)(j)
      }
      route += "\n"
    }
    println(route)
  }

  //Method printing leafset of the node
  def showLeafset() =
    {
      var leaves: String = ""
      leaves += " Node " + myID + "'s leafset:\n"
      for (i <- 0 to leafSet.length - 1) {
        for (j <- 0 to leafSet(i).length - 1) {
          leaves += "  at pos[" + i + "][" + j + "] :"
          if (leafSet(i)(j) != null)
            leaves += leafSet(i)(j)
        }
        leaves += "\n"
      }
      leaves += "\n"
      println(leaves)
    }

  //method getting best match in the routing table and returns NodeRefId
  //here rowIndex is same as the matchCount for newNodeId and the current nodeId
  def getFwdNodefromRoutingTable(nodeId: String, rowIndex: Int): String =
    {
      var rowNo: Int = rowIndex
      var colIndex: Int = 0
      if (rowIndex == 0) {
        colIndex = 0
      } else {
        colIndex = rowIndex - 1
      }
      var columnNo: Int = nodeId.charAt(colIndex).toString().toInt
      return routingTable(rowNo)(columnNo)
    }

  //method which performs prefix match
  def matchPrefixes(firstID: String, secondID: String): Int =
    {
      var firstIdSeq: Seq[Char] = firstID;
      var secIdSeq: Seq[Char] = secondID;
      var count = 0;
      for (i <- 0 to firstIdSeq.length - 1) {
        if (firstIdSeq(i).!=(secIdSeq(i))) {
          return count
        } else {
          count += 1
        }
      }
      return count
    }

  def adjustPosInLeafSet(rowIndex: Int, newNodeID: String): Unit =
    {
      var currNodeDiff = 0
      var newDiff = Math.abs(newNodeID.toInt - myID.toInt)
      var pos = -1

      if (!isArrayFull(leafSet(rowIndex))) {

        for (i <- 0 to leafSet(rowIndex).length - 1) {
          if (newNodeID.equalsIgnoreCase(leafSet(rowIndex)(i)))
            return
        }
        for (i <- 0 to leafSet(rowIndex).length - 1) {
          if (leafSet(rowIndex)(i) == null) {
            leafSet(rowIndex)(i) = newNodeID
            return
          }
        }
      } else {
        for (i <- 0 to leafSet(rowIndex).length - 1) {
          currNodeDiff = Math.abs(leafSet(rowIndex)(i).toInt - myID.toInt)
          if (newDiff < currNodeDiff) {
            newDiff = currNodeDiff
            pos = i
          }
        }
      }

      if (pos != -1)
        leafSet(rowIndex)(pos) = newNodeID

    }

  def isArrayFull(checkArray: Array[String]): Boolean =
    {
      for (i <- 0 to checkArray.length - 1) {
        if (checkArray(i) == null)
          return false
      }

      return true
    }

  //method maintaining leafset
  def adjustLeafset(newNodeID: String) {
    if (newNodeID.toInt < myID.toInt) {
      adjustPosInLeafSet(0, newNodeID)
    } else {
      adjustPosInLeafSet(1, newNodeID)
    }
  }
}