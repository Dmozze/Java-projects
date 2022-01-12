package info.kgeorgiy.ja.mozzhevilov.student;

import info.kgeorgiy.java.advanced.student.*;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class StudentDB implements AdvancedQuery {

  private static String getStudentsFirstNames(final Student x) {
    return x == null ? "" : x.getFirstName() + " " + x.getLastName();
  }

  private static <T extends Collection<R>, R> T getCollectionByMappingFunction(final Function<Student, R> function, final List<Student> students, final Supplier<T> collection) {
    return students.stream().map(function).collect(Collectors.toCollection(collection));
  }

  private static List<Student> getSortedListOfStudents(final Collection<Student> students, final Comparator<Student> cmp) {
    return students.stream().sorted(cmp).collect(Collectors.toList());
  }

  private static Stream<Student> getStreamFilteredByFunc(final Collection<Student> students, final Predicate<Student> predicate) {
    return students.stream().filter(predicate).sorted(Comparator.comparing(Student::getLastName, Comparator.reverseOrder()).
            thenComparing(Student::getFirstName, Comparator.reverseOrder()).thenComparing(Student::compareTo));
  }

  private <S, T> Predicate<S> getPredicate(final Function<S, T> getter, final T value) {
    return x -> getter.apply(x).equals(value);
  }

  private <T> List<Student> getListFilteredByField(final Collection<Student> students, final Function<Student, T> getter, final T value) {
    return getStreamFilteredByFunc(students, x -> getter.apply(x).equals(value)).collect(Collectors.toList());
  }

  private static List<Group> getListOfStudentsSlittedByGroup(final Collection<Student> students) {
    return students.stream()
            .collect(Collectors.groupingBy(Student::getGroup, TreeMap::new, Collectors.toList()))
            .entrySet().stream().map(x -> new Group(x.getKey(), x.getValue())).collect(Collectors.toList());
  }


  private Comparator<Group> makeComparator(final Function<Group, Long> function) {
    return Comparator.comparing(function);
  }

  private static GroupName getMaxByCmp(final Collection<Student> students, final Comparator<Group> cmp) {
    return getListOfStudentsSlittedByGroup(students).stream().max(cmp).map(Group::getName).orElse(null);
  }

  private Function<Group, Long> getFunctionByMap(final Map<GroupName, Long> map) {
    return x -> map.getOrDefault(x.getName(), -1L);
  }

  private String getMostPopularNameByMap(final Collection<Student> students, final Map<String, Long> map) {
    return students.stream().max(Comparator.comparing(
            (Student s) -> map.getOrDefault(s.getFirstName(), -1L))
            .thenComparing(Student::getFirstName)).map(Student::getFirstName).orElse("");
  }

  private <T> List<T> getListByIndices(final int[] indices, final Collection<Student> students, final Function<Student, T> function) {
    return IntStream.of(indices)
            .mapToObj(students.stream().limit(IntStream.of(indices).max().orElse(-1) + 1).collect(Collectors.toList())::get)
            .map(function).collect(Collectors.toList());
  }


  @Override
  public List<String> getFirstNames(final List<Student> students) {
    return getCollectionByMappingFunction(Student::getFirstName, students, ArrayList::new);
  }

  @Override
  public List<String> getLastNames(final List<Student> students) {
    return getCollectionByMappingFunction(Student::getLastName, students,  ArrayList::new);
  }

  @Override
  public List<GroupName> getGroups(final List<Student> students) {
    return getCollectionByMappingFunction(Student::getGroup, students, ArrayList::new);
  }

  @Override
  public List<String> getFullNames(final List<Student> students) {
    return getCollectionByMappingFunction(StudentDB::getStudentsFirstNames, students, ArrayList::new);
  }

  @Override
  public Set<String> getDistinctFirstNames(final List<Student> students) {
    return getCollectionByMappingFunction(Student::getFirstName, students, TreeSet::new);
  }

  @Override
  public String getMaxStudentFirstName(final List<Student> students) {
    return students.stream().max(Student::compareTo).map(Student::getFirstName).orElse("");
  }

  @Override
  public List<Student> sortStudentsById(final Collection<Student> students) {
    return getSortedListOfStudents(students, Student::compareTo);
  }

  @Override
  public List<Student> sortStudentsByName(final Collection<Student> students) {
    return getSortedListOfStudents(students, Comparator.comparing(Student::getLastName, Comparator.reverseOrder()).
            thenComparing(Student::getFirstName, Comparator.reverseOrder()).thenComparing(Student::compareTo));
  }

  @Override
  public List<Student> findStudentsByFirstName(final Collection<Student> students, final String name) {
    return getListFilteredByField(students, Student::getFirstName, name);
  }

  @Override
  public List<Student> findStudentsByLastName(final Collection<Student> students, final String name) {
    return getListFilteredByField(students, Student::getLastName, name);
  }

  @Override
  public List<Student> findStudentsByGroup(final Collection<Student> students, final GroupName group) {
    return getListFilteredByField(students, Student::getGroup, group);
  }

  @Override
  public Map<String, String> findStudentNamesByGroup(final Collection<Student> students, final GroupName group) {
    return getStreamFilteredByFunc(students, getPredicate(Student::getGroup, group))
            .collect(Collectors.toMap
                    (Student::getLastName, Student::getFirstName, BinaryOperator.minBy(String::compareTo)));
  }

  @Override
  public List<Group> getGroupsByName(final Collection<Student> students) {
    return getListOfStudentsSlittedByGroup(sortStudentsByName(students));
  }

  @Override
  public List<Group> getGroupsById(final Collection<Student> students) {
    return getListOfStudentsSlittedByGroup(sortStudentsById(students));
  }

  @Override
  public GroupName getLargestGroup(final Collection<Student> students) {
    // :NOTE: Память?
    return getMaxByCmp(students, makeComparator(group -> (long) group.getStudents().size()).thenComparing(Group::getName));
  }

  @Override
  public GroupName getLargestGroupFirstName(final Collection<Student> students) {
    return getMaxByCmp(students, makeComparator(getFunctionByMap( students.stream()
            .collect(Collectors.groupingBy(Student::getGroup, TreeMap::new, Collectors.toList()))
            .entrySet().stream().map(x -> new Group(x.getKey(), x.getValue())).collect(Collectors.toList()).stream()
            .collect(Collectors.toMap(Group::getName, x ->
                    x.getStudents().stream().map(Student::getFirstName).distinct().count()))))
            .thenComparing(Group::getName, Collections.reverseOrder()));
  }

  @Override
  public String getMostPopularName(final Collection<Student> students) {
    return getMostPopularNameByMap(
            students,
            students.stream()
                    .collect(Collectors.groupingBy(
                            Student::getFirstName,
                            Collectors.mapping(
                                    Student::getGroup,
                                    Collectors.collectingAndThen(Collectors.toSet(), x -> (long) x.size()))
                    ))
    );
  }

  @Override
  public List<String> getFirstNames(final Collection<Student> students, final int[] indices) {
    return getListByIndices(indices, students, Student::getFirstName);
  }

  @Override
  public List<String> getLastNames(final Collection<Student> students, final int[] indices) {
    return getListByIndices(indices, students, Student::getLastName);
  }

  @Override
  public List<GroupName> getGroups(final Collection<Student> students, final int[] indices) {
    return getListByIndices(indices, students, Student::getGroup);
  }

  @Override
  public List<String> getFullNames(final Collection<Student> students, final int[] indices) {
    return getListByIndices(indices, students, StudentDB::getStudentsFirstNames);
  }
}