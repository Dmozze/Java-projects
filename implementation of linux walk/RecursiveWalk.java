package info.kgeorgiy.ja.mozzhevilov.walk;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

public class RecursiveWalk {

  protected static final int BUFFER_SIZE = 1 << 4;

  protected static class RecursiveWalkException extends IOException {
    RecursiveWalkException(final String message, Exception error) {
      super(message + System.lineSeparator() + error.getMessage());
    }
  }

  protected static void printHash(BufferedWriter writer, long hash, String path) throws RecursiveWalkException {
    try {
      writer.write(String.format("%016x %s%n", hash, path));
    } catch (IOException e) {
      throw new RecursiveWalkException("Couldn't print answer", e);
    }
  }

  protected static long getHashFromThePath(Path path) {
    try (InputStream inputStream = Files.newInputStream(path)) {
      long start = 0;
      byte[] buffer = new byte[BUFFER_SIZE];
      for (int bytesreaded; (bytesreaded = inputStream.read(buffer, 0, BUFFER_SIZE)) >= 0; ) {
        for (int i = 0; i < bytesreaded; i++) {
          start = (start << 8) + (buffer[i] & 0xff);
          final long high = start & 0xff00_0000_0000_0000L;
          if (high != 0) {
            start ^= high >> 48;
            start &= ~high;
          }
        }
      }
      return start;
    } catch (InvalidPathException e) {
      System.err.println("Invalid path to" + path.toString() + ". " + e.getMessage());
      return 0L;
    } catch (IOException e) {
      System.err.println("Error while opening file to hash -  " + path.toString() + ". " + e.getMessage());
      return 0L;
    }
  }


  public static class OverrideVisitor extends SimpleFileVisitor<Path> {
    private final BufferedWriter output;

    OverrideVisitor(BufferedWriter writer) {
      output = writer;
    }

    @Override
    public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
      printHash(output, getHashFromThePath(path), path.toString());
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path path, IOException e) throws IOException {
      printHash(output, 0, path.toString());
      return FileVisitResult.CONTINUE;
    }
  }


  protected static Path getPath(String nameOfFile) throws RecursiveWalkException {
    try {
      return Paths.get(nameOfFile);
    } catch (InvalidPathException e) {
      throw new RecursiveWalkException("Invalid path to file " + nameOfFile, e);
    }
  }

  protected static void SolveTasksByWalking(final String inputFileName, final String outputFileName, final boolean isWalk) throws RecursiveWalkException {
    final Path input = getPath(inputFileName);
    final Path output = getPath(outputFileName);
    try {
      final Path parent = output.getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
    } catch (IOException e) {
      System.err.println("Error with creating output file");
    }

    try (BufferedReader in = Files.newBufferedReader(input)) {
      try (BufferedWriter out = Files.newBufferedWriter(output)) {
        String path;
        while ((path = in.readLine()) != null) {
          try {
            final File temp = new File(path);
            if (isWalk && temp.isDirectory()) {
              printHash(out, 0, path);
            } else {
              Files.walkFileTree(Paths.get(path), new OverrideVisitor(out));
            }
          } catch (InvalidPathException e) {
            printHash(out, 0, path);
            System.err.println("Invalid path " + path + e.getMessage());
          } catch (IOException e) {
            printHash(out, 0, path);
            System.err.println("Reading error" + path + e.getMessage());
          }
        }
      } catch (IOException e) {
        throw new RecursiveWalkException("Error occurred during work with input file", e);
      }
    } catch (IOException e) {
      throw new RecursiveWalkException("Error occurred during work with output file", e);
    }
  }

  public static void main(String[] args) {
    if (args == null || args.length != 2 || args[0] == null || args[1] == null) {
      System.err.println("Type only name of two files");
      return;
    }
    try {
      SolveTasksByWalking(args[0], args[1], false);
    } catch (RecursiveWalkException e) {
      System.err.println(e.getMessage());
    }
  }
}
