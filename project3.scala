object project3 {
  def main(args:Array[String])
  {
    if(args.length == 2)
    {
      new BossActor(args(0).toInt,args(1).toInt).start()
    }
    else
      println("Please enter no of nodes and no of Requests to be in the network.\nFormat Pastry <noOfNodes> <noOfRequests>")
  }

}