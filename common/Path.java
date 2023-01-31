package common;

import java.io.*;
import java.util.*;
import javax.management.openmbean.CompositeData;

/** Distributed filesystem paths.

    <p>
    Objects of type <code>Path</code> are used by all filesystem interfaces.
    Path objects are immutable.

    <p>
    The string representation of paths is a forward-slash-delimeted sequence of
    path components. The root directory is represented as a single forward
    slash.
    <p>
    The colon (<code>:</code>) and forward slash (<code>/</code>) characters are
    not permitted within path components. The forward slash is the delimeter,
    and the colon is reserved as a delimeter for application use.
 */
public class Path implements Iterable<String>, Serializable, Comparable<Path> {

  // The path is represented by an array of nodes, where each node
  // is either another directory (list) or a file (string).
  public List<String> p = new ArrayList<String>(); // The root dir is: ["/"]

  /** Creates a new path which represents the root directory. */
  public Path() {
    p.add("/");
  }

  /**
   * Creates a new path which is the concatination of the string array
   * (Used only by programmer)
   * @param pathNames The array of strings that get concatenated into a path
   * @throws IllegalArgumentException If pathNames is empty or starts with "/"
   */
  public Path(List<String> pathNames) {
    if (pathNames.isEmpty()) {
      throw new IllegalArgumentException("pathNames cannot be empty.");
    }
    if (!pathNames.get(0).equals("/")) {
      throw new IllegalArgumentException("pathNames must start with root.");
    }

    p.addAll(pathNames); // Add the rest of the strings
  }

  /** Creates a new path by appending the given component to an existing path.

        @param path The existing path.
        @param component The new component.
        @throws IllegalArgumentException If <code>component</code> includes the
                                         separator, a colon, or
                                         <code>component</code> is the empty
                                         string.
    */
  public Path(Path path, String component) {
    // Do checks
    if (component.contains(":")) {
      throw new IllegalArgumentException("Component contains colon.");
    }
    if (component.contains("/")) {
      throw new IllegalArgumentException("Component contains the separator.");
    }
    if (component.isBlank()) {
      throw new IllegalArgumentException("Component is blank");
    }

    // Create new list for the final value
    List<String> newList = new ArrayList<String>();
    // Add all old elements into new list
    newList.addAll(path.p);
    // Create new nodes for each element in component string
    List<String> compList = Arrays.asList(component.split("/")); // This will never do anything because separators aren't allowed
    newList.addAll(compList);
    this.p = newList;
  }

  /** Creates a new path from a path string.

        <p>
        The string is a sequence of components delimited with forward slashes.
        Empty components are dropped. The string must begin with a forward
        slash.

        @param path The path string.
        @throws IllegalArgumentException If the path string does not begin with
                                         a forward slash, or if the path
                                         contains a colon character.
     */
  public Path(String path) {
    if (!path.startsWith("/")) {
      throw new IllegalArgumentException(
        "Path doesn't start with forward slash."
      );
    }
    if (path.contains(":")) {
      throw new IllegalArgumentException("Path contains colon.");
    } else {
      try {
        // Create root node
        List<String> newRoot = new ArrayList<String>();
        newRoot.add("/");

        // Add the path
        List<String> newPath = new ArrayList<String>(
          Arrays.asList(path.split("/"))
        );

        // Remove empty elements
        if (newPath.size() > 1) {
          // newPath = newPath.subList(1, newPath.size());
          newPath.removeIf(x -> (x.equals("")));
        }

        // Create the path out of these element
        newRoot.addAll(newPath);
        p = newRoot;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Gets the path as a list of strings, starting with "/" for root
   * @return the list of components
   */
  public List<String> pathAsList() {
    return this.p;
  }

  /** Returns an iterator over the components of the path.

        <p>
        The iterator cannot be used to modify the path object - the
        <code>remove</code> method is not supported.

        @return The iterator.
     */
  @Override
  public Iterator<String> iterator() throws UnsupportedOperationException {
    // Implementing Iterator class to override its remove function
    final class NonRemoveIterable<E> implements Iterable<E> {

      private List<E> list = new ArrayList<E>();
      private int size = 0;

      public NonRemoveIterable(List<E> newList) {
        this.list = newList;
        this.size = newList.size();
      }

      public Iterator<E> iterator() {
        return new NonRemoveIterator<>();
      }

      final class NonRemoveIterator<T> implements Iterator<T> {

        int index = 0;

        @Override
        public boolean hasNext() {
          return index < list.size();
        }

        @Override
        public T next() {
          if (hasNext()) {
            T val = (T) list.get(index);
            index += 1;
            return val;
          } else {
            throw new NoSuchElementException();
          }
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException("remove not allowed.");
        }
      }
    }

    return new NonRemoveIterable<String>(this.p.subList(1, p.size()))
      .iterator();
  }

  /** Lists the paths of all files in a directory tree on the local
        filesystem.

        @param directory The root directory of the directory tree.
        @return An array of RELATIVE paths, one for each file in the directory
                tree.
        @throws FileNotFoundException If the root directory does not exist.
        @throws IllegalArgumentException If <code>directory</code> exists but
                                         does not refer to a directory.
     */
  public static Path[] list(File directory) throws FileNotFoundException {
    if (!directory.exists()) { // Do we need to check if it's root?
      throw new FileNotFoundException("Directory does not exist");
    }
    if (!directory.isDirectory()) {
      throw new IllegalArgumentException(
        "Argument does not refer to a directory"
      );
    }

    // Array for dynamic sizing
    List<Path> pathsList = new ArrayList<Path>();

    for (File f : directory.listFiles()) {
      Path currentPath = new Path("/" + f.getName());

      // If f is a file, we can just add it to the list
      if (f.isFile()) {
        pathsList.add(currentPath);
      }
      // If f is a directory, we need to recursively list its elements
      else if (f.isDirectory()) {
        List<Path> newPathsArray = Arrays.asList(list(f));
        // Add prefix of the current directory to each recursive new paths
        newPathsArray = addDirPrefix(currentPath, newPathsArray);
        // Add all the new paths
        pathsList.addAll(newPathsArray);
      } else {
        System.out.println("File " + f + " is neither dir nor file");
      }
    }

    // Convert back to array for type-checking
    Path[] pathsArray = new Path[pathsList.size()];
    for (int i = 0; i < pathsArray.length; i++) {
      pathsArray[i] = pathsList.get(i);
    }

    return pathsArray;
  }

  /**
   * For each path in a list, add a prefix of a directory
   * @param dirPath The directory prefix to add
   * @param paths The list of paths to add the prefix to
   * @return The updated list of paths
   */
  public static List<Path> addDirPrefix(Path dirPath, List<Path> paths) {
    String dirName = dirPath.p.get(1);

    for (Path path : paths) {
      // Add the directory name at the START of the path list
      path.p.add(1, dirName);
    }

    return paths;
  }

  /** Determines whether the path represents the root directory.

        @return <code>true</code> if the path does represent the root directory,
                and <code>false</code> if it does not.
     */
  public boolean isRoot() {
    // A path is the root if it's a list that contains only the string "/"
    return p.size() == 1 && p.get(0).equals("/");
  }

  /** Returns the path to the parent of this path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no parent.
     */
  public Path parent() {
    if (isRoot()) {
      throw new IllegalArgumentException("Path represents root.");
    }
    // Remove the last element in the path array to get its parent
    return new Path(p.subList(0, p.size() - 1));
  }

  /** Returns the last component in the path.

        @throws IllegalArgumentException If the path represents the root
                                         directory, and therefore has no last
                                         component.
     */
  public String last() {
    if (isRoot()) {
      throw new IllegalArgumentException("Path represents root.");
    }

    return p.get(p.size() - 1);
  }

  /** Determines if the given path is a subpath of this path.

        <p>
        The other path is a subpath of this path if is a prefix of this path.
        Note that by this definition, each path is a subpath of itself. 
        In other words, a subpath must start from the beginning of the path.

        @param other The path to be tested.
        @return <code>true</code> If and only if the other path is a subpath of
                this path.
     */
  public boolean isSubpath(Path other) {
    // Check if other length is greater than path length
    if (other.p.size() > this.p.size()) {
      return false;
    }
    // Check if other is a prefix of this
    return other.p.equals(this.p.subList(0, other.p.size()));
  }

  /** Converts the path to <code>File</code> object.

        @param root The resulting <code>File</code> object is created relative
                    to this directory.
        @return The <code>File</code> object.
     */
  public File toFile(File root) {
    String pathname = "";

    for (String name : this.p) {
      // String representation of the root path is "/",
      // so you have to make sure not to add another "/" to it
      if (!name.equals("/")) {
        pathname = pathname.concat(name).concat("/");
      }
    }

    return new File(root, pathname);
  }

  /** Compares two paths for equality.

        <p>
        Two paths are equal if they share all the same components.

        @param other The other path.
        @return <code>true</code> if and only if the two paths are equal.
     */
  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null) {
      return false;
    }
    if (this.getClass() != other.getClass()) {
      return false;
    }

    return this.p.equals(((Path) other).p);
  }

  /** Returns the hash code of the path. */
  @Override
  public int hashCode() {
    return this.p.hashCode();
  }

  /** Converts the path to a string.

        <p>
        The string may later be used as an argument to the
        <code>Path(String)</code> constructor.

        @return The string representation of the path.
     */
  @Override
  public String toString() {
    String str = this.p.toString();

    // Remove opening and closing brackets
    str = str.substring(1, str.length() - 1);

    // Replace commas with forward slash
    str = str.replaceAll(", ", "/");

    // Remove first "/" if the path isn't root
    if (!this.isRoot()) {
      str = str.substring(1);
    }

    return str;
  }

  /* Project 2 new function */

  /** Compares this path to another.

        <p>
        An ordering upon <code>Path</code> objects is provided to prevent
        deadlocks between applications that need to lock multiple filesystem
        objects simultaneously. By convention, paths that need to be locked
        simultaneously are locked in increasing order.

        <p>
        Because locking a path requires locking every component along the path,
        the order is not arbitrary. For example, suppose the paths were ordered
        first by length, so that <code>/etc</code> precedes
        <code>/bin/cat</code>, which precedes <code>/etc/dfs/conf.txt</code>.

        <p>
        Now, suppose two users are running two applications, such as two
        instances of <code>cp</code>. One needs to work with <code>/etc</code>
        and <code>/bin/cat</code>, and the other with <code>/bin/cat</code> and
        <code>/etc/dfs/conf.txt</code>.

        <p>
        Then, if both applications follow the convention and lock paths in
        increasing order, the following situation can occur: the first
        application locks <code>/etc</code>. The second application locks
        <code>/bin/cat</code>. The first application tries to lock
        <code>/bin/cat</code> also, but gets blocked because the second
        application holds the lock. Now, the second application tries to lock
        <code>/etc/dfs/conf.txt</code>, and also gets blocked, because it would
        need to acquire the lock for <code>/etc</code> to do so. The two
        applications are now deadlocked.

        <p>
        As a general rule to prevent this scenario, the ordering is chosen so
        that objects that are near each other in the path hierarchy are also
        near each other in the ordering. That is, in the above example, there is
        not an object such as <code>/bin/cat</code> between two objects that are
        both under <code>/etc</code>.

        @param other The other path.
        @return Zero if the two paths are equal, a negative number if this path
                precedes the other path, or a positive number if this path
                follows the other path.
     */
  @Override
  public int compareTo(Path other) {
    if (this.equals(other)) {
      return 0;
    }

    if (this.isSubpath(other)) {
      return 1;
    }

    return -1;
  }
}
