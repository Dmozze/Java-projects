package info.kgeorgiy.ja.mozzhevilov.implementor;

import info.kgeorgiy.java.advanced.implementor.ImplerException;
import info.kgeorgiy.java.advanced.implementor.JarImpler;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.*;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;


public class Implementor implements JarImpler {
  public Implementor() {
  }


  private static class PlsFasterBoy extends SimpleFileVisitor<Path> {

    public PlsFasterBoy() {
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      Files.delete(file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      Files.delete(dir);
      return FileVisitResult.CONTINUE;
    }
  }

  private static class MethodWrapper {

    private final Method method;

    private MethodWrapper(Method method) {
      this.method = method;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Method that = ((MethodWrapper) o).getMethod();
      return Arrays.equals(method.getParameterTypes(), that.getParameterTypes()) &&
              method.getReturnType().equals(that.getReturnType()) &&
              method.getName().equals(that.getName());
    }
    @Override
    public int hashCode() {
      return Objects.hash(Arrays.hashCode(method.getParameterTypes()),
              method.getReturnType().hashCode(),
              method.getName().hashCode());
    }


    public Method getMethod() {
      return method;
    }

  }


  private String pakage(Class<?> token) {
    StringBuilder res = new StringBuilder();
    if (!"".equals(token.getPackageName())) {
      res.append("package ").append(token.getPackageName()).append(';').append(System.lineSeparator());
    }
    res.append(System.lineSeparator());
    return res.toString();
  }


  private String getReturnTypeAndName(Class<?> token, Executable executable) {
    if (executable instanceof Method) {
      Method tmp = (Method) executable;
      return tmp.getReturnType().getCanonicalName() + ' ' + tmp.getName();
    } else {
      return token.getSimpleName() + "Impl";
    }
  }

  private String take(Executable executable) {
    StringBuilder res = new StringBuilder();
    Class<?>[] exceptions = executable.getExceptionTypes();
    if (exceptions.length > 0) {
      res.append(" throws ");
    }
    res.append(Arrays.stream(exceptions)
            .map(Class::getCanonicalName)
            .collect(Collectors.joining(", ")));
    return res.toString();
  }

  private String getDefaultValue(Class<?> token) {
    if (boolean.class.equals(token)) {
      return " false";
    } else if (void.class.equals(token)) {
      return "";
    } else if (token.isPrimitive()) {
      return " 0";
    }
    return " null";
  }

  private String getBody(Executable executable) {
    if (executable instanceof Method) {
      return "return" + getDefaultValue(((Method) executable).getReturnType()) + ';';
    } else {
      return "super" +  Arrays.stream(executable.getParameters())
              .map(parameter -> ("") + parameter.getName())
              .collect(Collectors.joining(", ", "(", ")")) + ';';
    }
  }

  private String takeExec(Class<?> token, Executable executable) {
    StringBuilder res = new StringBuilder("    ".repeat(1));
    final int mods = executable.getModifiers() & ~Modifier.ABSTRACT & ~Modifier.NATIVE & ~Modifier.TRANSIENT;
    res.append(Modifier.toString(mods))
            .append(mods > 0 ? " " : "")
            .append(getReturnTypeAndName(token, executable))
            .append( Arrays.stream(executable.getParameters())
                    .map(parameter -> (parameter.getType().getCanonicalName() + ' ') + parameter.getName())
                    .collect(Collectors.joining(", ", "(", ")")))
            .append(take(executable))
            .append(" {")
            .append(System.lineSeparator())
            .append("    ".repeat(2))
            .append(getBody(executable))
            .append(System.lineSeparator())
            .append("    ".repeat(1))
            .append("}")
            .append(System.lineSeparator());
    return res.toString();
  }

  private void addMethodsToSet(Method[] methods, Set<MethodWrapper> storage) {
    Arrays.stream(methods)
            .map(MethodWrapper::new)
            .forEach(storage::add);
  }


  private void implementAbstractMethods(Class<?> token, Writer writer) throws IOException {
    Set<MethodWrapper> methods = new HashSet<>();
    addMethodsToSet(token.getMethods(), methods);
    while (token != null) {
      addMethodsToSet(token.getDeclaredMethods(), methods);
      token = token.getSuperclass();
    }
    for (MethodWrapper methodWrapper : methods.stream()
            .filter(methodWrapper -> Modifier.isAbstract(methodWrapper.getMethod().getModifiers()))
            .collect(Collectors.toSet())) {
      writer.write(toUni(takeExec(null, methodWrapper.getMethod())));
    }
  }


  private void implementConstructors(Class<?> token, Writer writer) throws ImplerException, IOException {
    Constructor<?>[] constructors = Arrays.stream(token.getDeclaredConstructors())
            .filter(constructor -> !Modifier.isPrivate(constructor.getModifiers()))
            .toArray(Constructor[]::new);
    if (constructors.length < 1) {
      throw new ImplerException("Non-private const aren't normal");
    }
    for (Constructor<?> constructor : constructors) {
      writer.write(toUni(takeExec(token, constructor)));
    }
  }

  @Override
  public void implement(Class<?> token, Path root) throws ImplerException {
    checkNotNull(token, root);
    if (token.isPrimitive() || token.isArray() || token == Enum.class || Modifier.isFinal(token.getModifiers()) || Modifier.isPrivate(token.getModifiers())) {
      throw new ImplerException("Incorrect token");
    }
    root = getPath(token, root, ".java");
    createDirectories(root);

    try (BufferedWriter writer = Files.newBufferedWriter(root)) {
      try {
        writer.write(toUni(pakage(token) + "public class " + token.getSimpleName() + "Impl" + ' ' +
                (token.isInterface() ? "implements " : "extends ") +
                token.getCanonicalName() + " {" + System.lineSeparator()));
        if (!token.isInterface()) {
          implementConstructors(token, writer);
        }
        implementAbstractMethods(token, writer);
        writer.write('}' + System.lineSeparator());
      } catch (IOException e) {
        throw new ImplerException("troubles with output", e);
      }
    } catch (IOException e) {
      throw new ImplerException("troubles with input", e);
    }
  }

  private void createDirectories(Path root) {
    if (root.getParent() != null) {
      try {
        Files.createDirectories(root.getParent());
      } catch (IOException ignored) {
      }
    }
  }

  private Path getPath(Class<?> token, Path root, String end) {
    return root.resolve(token.getPackageName().replace('.', File.separatorChar))
            .resolve(token.getSimpleName() + "Impl" + end);
  }

  private String toUni(String str) {
    StringBuilder b = new StringBuilder();
    for (char c : str.toCharArray()) {
      if (c >= 128)
        b.append(String.format("\\u%04X", (int) c));
      else
        b.append(c);
    }
    return b.toString();
  }

  private String getClassPath(final Class<?> token) throws ImplerException {
    try {
      return Path.of(token.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
    } catch (URISyntaxException e) {
      throw new ImplerException("Error while getting path for class token", e);
    }
  }

  @Override
  public void implementJar(Class<?> token, Path root) throws ImplerException {
    checkNotNull(token, root);
    createDirectories(root);
    final Path tempDir;
    try {
      tempDir = Files.createTempDirectory(root.toAbsolutePath().getParent(), "temp");
    } catch (IOException e) {
      throw new ImplerException("Can not create directory", e);
    }
    implement(token, tempDir);
    final JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
    if (javaCompiler == null || javaCompiler.run(null, null, null, "-cp",
            tempDir.toString() + File.pathSeparator + System.getProperty("java.class.path") +
                    File.pathSeparator + getClassPath(token),
            getPath(token, tempDir, ".java").toString()) != 0) {
      throw new ImplerException("Error while compiling");
    }
    try (JarOutputStream jarOutputStream = new JarOutputStream(Files.newOutputStream(root), new Manifest())) {
      jarOutputStream.putNextEntry(new ZipEntry(zipName(token)));
      Files.copy(getPath(token, tempDir, ".class"), jarOutputStream);
    } catch (IOException ignored) {
    }
  }

  private String zipName(Class<?> token) {
    return token.getPackageName().replace('.', '/')
            + '/' + token.getSimpleName() + "Impl"
            + ".class";
  }

  private void checkNotNull(Class<?> token, Path root) throws ImplerException {
    if (token == null || root == null) {
      throw new ImplerException("token or path are null");
    }
  }

 public static void main(String[] args) throws ClassNotFoundException {
    if (args == null || args.length != 2) {
      return;
    }
    if (Arrays.stream(args).anyMatch(Objects::isNull)) {
        return;
    }
    try {
      final JarImpler implementor = new Implementor();
      implementor.implement(Class.forName(args[0]), Paths.get(args[1]));
    } catch (ImplerException e) {
      e.printStackTrace();
    }
 }
}
