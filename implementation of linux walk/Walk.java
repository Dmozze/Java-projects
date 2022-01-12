package info.kgeorgiy.ja.mozzhevilov.walk;

public class Walk extends RecursiveWalk {
  public static void main(String[] args) {
    if (args == null || args.length != 2 || args[0] == null || args[1] == null) {
      System.err.println("Type only name of two files");
      return;
    }
    try {
      SolveTasksByWalking(args[0], args[1], true);
    } catch (RecursiveWalkException e) {
      System.err.println(e.getMessage());
    }
  }
}
