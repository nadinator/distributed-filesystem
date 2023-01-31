package naming;

import common.*; // Path stuff
import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Semaphore;
import rmi.*; // Rmi stuff
import storage.*; // SS stuff

/** Naming server.

    <p>
    Each instance of the filesystem is centered on a single naming server. The
    naming server maintains the filesystem directory tree. It does not store any
    file data - this is done by separate storage servers. The primary purpose of
    the naming server is to map each file name (path) to the storage server
    which hosts the file's contents.

    <p>
    The naming server provides two interfaces, <code>Service</code> and
    <code>Registration</code>, which are accessible through RMI. Storage servers
    use the <code>Registration</code> interface to inform the naming server of
    their existence. Clients use the <code>Service</code> interface to perform
    most filesystem operations. The documentation accompanying these interfaces
    provides details on the methods supported.

    <p>
    Stubs for accessing the naming server must typically be created by directly
    specifying the remote network address. To make this possible, the client and
    registration interfaces are available at well-known ports defined in
    <code>NamingStubs</code>.
 */
public class NamingServer implements Service, Registration {

  // Naming Server variables
  public List<Node> tree = new ArrayList<>(); // NS directory tree
  public List<SS> storageServers = new ArrayList<>(); // List of running storage servers
  public Skeleton<Service> serviceSkel; // Service skeleton
  public Skeleton<Registration> regSkel; // Registration skeleton

  // Replication constants
  private static final double ALPHA = 0.2;
  private static final int REPLICA_GRANULARITY = 20;
  private static int REPLICA_UPPER_BOUND = 3;

  // Service Queue for both readers and writers that ensures ordering fairness
  public Semaphore serviceQueue = new Semaphore(1, true);

  // Communication stubs for a running Storage Server
  public class SS {

    Command cStub;
    Storage sStub;

    public SS(Command c, Storage s) {
      cStub = c;
      sStub = s;
    }

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

      String cs = cStub.toString();
      String ss = sStub.toString();
      String o_cs = ((SS) other).cStub.toString();
      String o_ss = ((SS) other).sStub.toString();

      return cs.equals(o_cs) && ss.equals(o_ss);
    }
  }

  // Nodes of the file tree
  private class Node {

    public String name; // name of file/directory
    public Semaphore resource = new Semaphore(1, true); // File lock
    public Semaphore rmutex = new Semaphore(1, true); // Readers lock
    public int readCount = 0; // Number of clients reading this node

    @Override
    public String toString() {
      return name;
    }
  }

  // Leaf nodes (files)
  public class Leaf extends Node {

    List<SS> replicaList = new ArrayList<>(); // List of SS's that contain replicas of this file
    SS most_recent_ss; // Most recent SS used to access this file (used for invalidation)
    int numRequests = 0; // Number of requests to a file
    int numRequestsCoarse = 0; // Number of coarse-grained requests (updated every REPLICA_GRANULARITY requests)

    /**
     * Returns a random storage server from the list of SS's that contain this file (replicaList)
     */
    public SS getRandomLeafSS() {
      Random rand = new Random();
      return replicaList.get(rand.nextInt(replicaList.size()));
    }

    /**
     * After an update to numRequests, this return how many new replicas need to be created
     * @return the number of new replicas to create
     */
    public int replicasToCreate() {
      // Update numRequestsCoarse
      if (numRequests % REPLICA_GRANULARITY == 0) {
        numRequestsCoarse = numRequests;
      }
      // Apply equation in handout
      int alphaCourse = Double.valueOf(ALPHA * numRequestsCoarse).intValue();
      int new_num_replicas = Math.min(alphaCourse, REPLICA_UPPER_BOUND);
      // Return amount of new replicas to create
      return (new_num_replicas - replicaList.size());
    }

    /**
     * Create a list of storage servers that you will replicate a file to
     * @return The random SS's assigned to hold the new replicas if more replicas need to be
     *         created (could be an empty list)
     * @throws InterruptedException
     */
    public List<SS> getReplicaStorageServers() throws InterruptedException {
      int new_replicas = replicasToCreate();
      List<SS> ssList = new ArrayList<>(); // Contains SSs that need to make a replica
      int i = 0;

      while (
        i < storageServers.size() &&
        (replicaList.size() + ssList.size()) < REPLICA_UPPER_BOUND &&
        ssList.size() < new_replicas
      ) {
        SS ss = storageServers.get(i);
        if (!replicaList.contains(ss)) {
          ssList.add(ss);
        }
        i++;
      }

      return ssList;
    }

    /**
     * Replicate a file by copying it to however many needed new SSs
     * @param p The path of the file to replicate
     * @throws FileNotFoundException
     * @throws RMIException
     * @throws IOException
     * @throws InterruptedException
     */
    public void replicate(Path p)
      throws FileNotFoundException, RMIException, IOException, InterruptedException {
      List<SS> ssList = getReplicaStorageServers();
      // Copy contents of file to new SS with replica, if new ones need to be created
      for (SS ss : ssList) {
        ss.cStub.copy(p, replicaList.get(0).sStub); // Copy from the first SS because that's where the file was registered
        replicaList.add(ss);
      }
    }
  }

  // Branch nodes (directories)
  public class Branch extends Node {

    List<Node> list = new ArrayList<>();

    public Branch(String name) {
      this.name = name;
    }
  }

  /** Creates the naming server object.

        <p>
        The naming server is not started.
     */
  public NamingServer() {
    // The tree initially has one branch, the root.
    tree.add(new Branch("/"));
  }

  /**
   * Returns the root node (i.e., the first element of the global variable "tree")
   * @throws IllegalStateException if the tree hasn't been initialized with a root
   */
  public Node getRootNode() throws IllegalStateException {
    if (tree.size() == 0) {
      throw new IllegalStateException("Root has not been added to tree!");
    } else {
      return tree.get(0);
    }
  }

  /** Starts the naming server.

        <p>
        After this method is called, it is possible to access the client and
        registration interfaces of the naming server remotely.

        @throws RMIException If either of the two skeletons, for the client or
                             registration server interfaces, could not be
                             started. The user should not attempt to start the
                             server again if an exception occurs.
     */
  public synchronized void start() throws RMIException {
    // Create registration skeleton at specified port
    NamingServer impl = new NamingServer();
    Class<Registration> r_intf = Registration.class;
    InetSocketAddress r_addr = new InetSocketAddress(
      NamingStubs.REGISTRATION_PORT
    );
    regSkel = new Skeleton<>(r_intf, impl, r_addr);

    // Attempt to start the registration skeleton
    try {
      regSkel.start();
    } catch (RMIException e) {
      System.err.println("Could not start registration skeleton.");
      throw e;
    }

    // Create service skel at specified port
    Class<Service> s_intf = Service.class;
    InetSocketAddress s_addr = new InetSocketAddress(NamingStubs.SERVICE_PORT);
    serviceSkel = new Skeleton<>(s_intf, impl, s_addr);

    // Attempt to start the service skeleton
    try {
      serviceSkel.start();
    } catch (RMIException e) {
      System.err.println("Could not start service skeleton.");
      throw e;
    }
  }

  /** Stops the naming server.

        <p>
        This method waits for both the client and registration interface
        skeletons to stop. It attempts to interrupt as many of the threads that
        are executing naming server code as possible. After this method is
        called, the naming server is no longer accessible remotely. The naming
        server should not be restarted.
     */
  public void stop() {
    try {
      serviceSkel.stop();
    } catch (Exception e) {
      System.err.println("Couldn't stop service skel");
      stopped(e);
    }
    try {
      regSkel.stop();
    } catch (Exception e) {
      System.err.println("Couldn't stop reg skel");
      stopped(e);
    }
    stopped(null);
  }

  /** Indicates that the server has completely shut down.

        <p>
        This method should be overridden for error reporting and application
        exit purposes. The default implementation does nothing.

        @param cause The cause for the shutdown, or <code>null</code> if the
                     shutdown was by explicit user request.
     */
  protected void stopped(Throwable cause) {
    if (cause != null) {
      System.err.println();
      System.err.println("stopped() report:");
      cause.printStackTrace();
    }
  }

  /**
   * Checks whether a given path exists in the directory tree and
   * returns the node containing that path if so, by checking each component
   * of the string to the elements' names in the current directory
   * @param t The directory we're checking for the path in
   * @param path The path to check, in its List<String> (true) form
   * @return The node of the path if it exists, null otherwise
   */
  public Node getNodeFromPath(List<Node> t, List<String> p) {
    // Base case 1: Path is empty
    if (p.size() == 0) {
      return null;
    }

    // Base case 2: Path is root
    if (p.size() == 1 && p.get(0).equals("/")) {
      return getRootNode(); // Return the root node
    }

    // We compare the node names to the first component of the path
    String component = p.get(0);

    for (Node n : t) {
      if (n.name.equals(component)) { // Search success
        if (p.size() == 1) { // This is the last component we're looking for
          return n;
        } else { // More to search; go inside directory
          if (n instanceof Branch) {
            return getNodeFromPath(((Branch) n).list, p.subList(1, p.size()));
          } else { // n is a file; the path is invalid
            return null;
          }
        }
      }
    }

    // Search failed
    return null;
  }

  // The following methods are documented in Service.java.
  @Override
  public boolean isDirectory(Path path) throws FileNotFoundException {
    Node n = getNodeFromPath(tree, path.pathAsList());
    if (n != null) {
      return (n instanceof Branch);
    } else {
      throw new FileNotFoundException();
    }
  }

  @Override
  public String[] list(Path directory) throws FileNotFoundException {
    if (isDirectory(directory)) {
      // Get the branch
      Branch b = (Branch) getNodeFromPath(tree, directory.pathAsList());
      // Get its list of names (absolute paths)
      String[] names = new String[b.list.size()];
      // Set the names list and return it
      for (int i = 0; i < names.length; i++) {
        names[i] = b.list.get(i).name;
      }
      return names;
    } else {
      throw new FileNotFoundException();
    }
  }

  /* Storage Server-communication functions */

  /**
   * Returns a random storage server from the storageServers list
   */
  public SS getRandomSS() {
    Random rand = new Random();
    return storageServers.get(rand.nextInt(storageServers.size()));
  }

  @Override
  public boolean createFile(Path file)
    throws RMIException, FileNotFoundException {
    Node n = getNodeFromPath(tree, file.pathAsList());
    // Check if file exists
    if (n == null) {
      // Get parent directory of file's node
      Node pn = getNodeFromPath(tree, file.parent().pathAsList());
      if (pn == null || pn instanceof Leaf) {
        throw new FileNotFoundException(
          "Parent directory does not exist or is not a directory."
        );
      }

      // Create new leaf for file
      Leaf l = new Leaf();
      l.name = file.last();

      // Add file to a random storage server
      SS ss = getRandomSS();
      if (!ss.cStub.create(file)) {
        System.out.println("Couldn't create " + file + " on SS " + ss);
        return false;
      }
      l.replicaList.add(ss);

      // Add file to its parent directory
      ((Branch) pn).list.add(l);
      return true;
    } else {
      // File with this path already exists
      return false;
    }
  }

  @Override
  public boolean createDirectory(Path directory) throws FileNotFoundException {
    // Check if directory exists
    Node n = getNodeFromPath(tree, directory.pathAsList());
    if (n == null) {
      // Create new branch for directory
      Branch b = new Branch(directory.last()); // Save directory name
      // Add directory to parent directory list
      try {
        Node pn = getNodeFromPath(tree, directory.parent().pathAsList());
        if (pn == null || pn instanceof Leaf) {
          throw new FileNotFoundException(
            "Parent directory does not exist or is not a directory."
          );
        }
        ((Branch) pn).list.add(b);
      } catch (Exception e) {
        throw e;
      }
      return true;
    } else {
      // Directory already exists
      return false;
    }
  }

  @Override
  public boolean delete(Path path) throws FileNotFoundException {
    Node n = getNodeFromPath(tree, path.pathAsList());
    if (n == null) {
      throw new FileNotFoundException();
    }

    if (n instanceof Leaf) { // Deleting a Leaf node
      Leaf l = (Leaf) n;
      // Remove this file from its parent branch
      try {
        Branch pd = (Branch) getNodeFromPath(tree, path.parent().pathAsList());
        if (!pd.list.remove(l)) {
          System.err.println(
            "Couldn't remove leaf node " + l + " from its parent " + pd
          );
          return false;
        }
        // Contact replica SSs to delete their replicas
        for (SS ss : l.replicaList) {
          if (!ss.cStub.delete(path)) {
            System.err.println("Couldn't delete file " + l + " from SS " + ss);
            return false;
          }
        }
      } catch (RMIException e) {
        e.printStackTrace();
        return false;
      }
    } else { // Deleting a Branch node
      Branch b = (Branch) n;
      // Remove this directory from its parent branch
      if (!path.isRoot()) { // Root doesn't have a parent
        try {
          for (SS ss : storageServers) {
            ss.cStub.delete(path);
          }
          Branch pd = (Branch) getNodeFromPath(
            tree,
            path.parent().pathAsList()
          );
          if (!pd.list.remove(b)) {
            System.err.println(
              "Couldn't remove branch node " + b + " from its parent " + pd
            );
            return false;
          }
        } catch (RMIException e) {
          e.printStackTrace();
          return false;
        }
      }
    }

    return true;
  }

  @Override
  public Storage getStorage(Path file) throws FileNotFoundException {
    Node n = getNodeFromPath(tree, file.pathAsList());
    if (n == null || n instanceof Branch) {
      throw new FileNotFoundException();
    }
    // Update most-recently-used SS for this file
    Leaf l = (Leaf) n;
    l.most_recent_ss = l.getRandomLeafSS(); // Direct request to a random storage server hosting the file
    return l.most_recent_ss.sStub;
  }

  /**
   * Checks if an SS is already registered by comparing its
   * stubs to those that exist in the list storageServers
   * @param c The command stub
   * @param s The storage stub
   * @return True if exists, else false
   */
  public boolean storageExists(Command c, Storage s) {
    for (SS ss : storageServers) {
      if (ss.cStub.equals(c) && ss.sStub.equals(s)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Adds a file that was presented by an SS during registration to the directory tree.
   * Only difference from createFile() is that this one creates a node's parent if it
   * doesn't find it, rather than throw FileNotFoundException.
   * @param file Path of file to be added
   * @param c Client command stub
   * @param s SS storage stub
   * @return true if file was added to the tree, false if it already exists
   */
  public boolean registerFile(Path file, SS ss) {
    Node n = getNodeFromPath(tree, file.pathAsList());

    if (n == null) { // Node doesn't already exist
      // Create new leaf for file
      Leaf l = new Leaf();
      l.name = file.last(); // name in tree is the name of the file itself

      // Get parent directory of file's node (all parents of nodes are branches)
      Branch pd = (Branch) getNodeFromPath(tree, file.parent().pathAsList());
      if (pd == null) { // Create the dir if doesn't exist
        registerDirectory(file.parent());
        pd = (Branch) getNodeFromPath(tree, file.parent().pathAsList());
      }

      // Add file node to its parent branch's list
      pd.list.add(l);

      // Update file's replication variables
      l.replicaList.add(ss);
      l.most_recent_ss = ss;

      return true;
    } else { // Node already exists
      return false;
    }
  }

  /**
   * Registers a directory. Only difference from createDirectory() is that
   * if it doesn't find the parent then it creates it, rather than throwing
   * a FileNotFoundException.
   * @param dir
   * @return true if created, false if already exists
   */
  public boolean registerDirectory(Path dir) {
    Node n = getNodeFromPath(tree, dir.pathAsList());

    if (n == null) { // Node doesn't already exist
      // Create new branch for directory
      Branch b = new Branch(dir.last());

      // Get parent directory
      Branch pd = (Branch) getNodeFromPath(tree, dir.parent().pathAsList());

      // Create the parent directory if not found
      if (pd == null) {
        registerDirectory(dir.parent());
        // Try again
        pd = (Branch) getNodeFromPath(tree, dir.parent().pathAsList());
      }

      // Add branch node to its parent branch's list
      pd.list.add(b);

      return true;
    } else {
      return false;
    }
  }

  // The method register is documented in Registration.java.
  @Override
  public Path[] register(
    Storage client_stub, // Client with SS
    Command command_stub, // NS with SS
    Path[] files
  ) {
    // Null check
    if (client_stub == null || command_stub == null || files == null) {
      throw new NullPointerException();
    }

    // Check if this SS already registered
    if (storageExists(command_stub, client_stub)) {
      throw new IllegalStateException();
    }

    // Create and add new SS to storageServers list
    SS ss = new SS(command_stub, client_stub);
    storageServers.add(ss);

    // Check each path for whether it exists in the tree
    // and create it if not
    List<Path> pathsToDelete = new ArrayList<>();
    for (Path p : files) {
      if (!p.isRoot()) { // Silently ignore requests to create root
        // Try to get a node in the tree from the path
        Node n = getNodeFromPath(tree, p.pathAsList());
        if (n != null) { // Found a path that already exists
          pathsToDelete.add(p);
        } else { // Path doesn't exist
          registerFile(p, ss);
        }
      }
    }

    // Convert deletion list to array (type-checking)
    Path[] ptdArray = new Path[pathsToDelete.size()];
    for (int i = 0; i < ptdArray.length; i++) {
      ptdArray[i] = pathsToDelete.get(i);
    }

    // Return the deletion list
    return ptdArray;
  }

  /* Project 2 new operations */

  /**
   * Lock a path.
   * The difference between this and lock() is that
   * this one doesn't require acquiring a serviceQueue mutex
   * and therefore is non-blocking.
   * @param path The path to lock
   * @throws FileNotFoundException
   * @throws RMIException
   */
  public void lockNonBlocking(Path path, boolean exclusive)
    throws FileNotFoundException, RMIException {
    Node n = getNodeFromPath(tree, path.pathAsList());

    if (n == null) {
      throw new FileNotFoundException();
    }

    try {
      if (!path.isRoot()) {
        // Non-exclusively lock parents
        lockNonBlocking(path.parent(), false);
      }

      n.rmutex.acquire();
      ++n.readCount;
      if (n.readCount == 1) {
        n.resource.acquire();
      }
      n.rmutex.release();
    } catch (InterruptedException e) {
      unlock(path, false);
      e.printStackTrace();
    } catch (Exception e) {
      unlock(path, false);
      throw e;
    }
  }

  /**
   * Checks if replication is needed after a file has been accessed,
   * and replicates it if so
   * @param n the node we're checking
   * @param p the node's path
   * @throws InterruptedException
   * @throws FileNotFoundException
   * @throws RMIException
   * @throws IOException
   */
  public void checkReplication(Node n, Path p)
    throws InterruptedException, FileNotFoundException, RMIException, IOException {
    if (n instanceof Leaf) { // We only replicate files
      Leaf l = (Leaf) n;
      l.numRequests++; // Update file's request counter
      l.replicate(p); // Replicate the file if needed
    }
  }

  @Override
  public void lock(Path path, boolean exclusive)
    throws RMIException, FileNotFoundException {
    Node n = getNodeFromPath(tree, path.pathAsList());

    if (n == null) {
      throw new FileNotFoundException();
    }

    try {
      serviceQueue.acquire();
      checkReplication(n, path);

      if (!path.isRoot()) {
        // Non-exclusively lock parents with the non-blocking helper function
        lockNonBlocking(path.parent(), false);
      }

      // write request
      if (exclusive) {
        n.resource.acquire();
      }
      // read request
      else {
        n.rmutex.acquire();
        ++n.readCount;
        // Only acquire the node's resource mutex if you're the first reader
        if (n.readCount == 1) {
          n.resource.acquire();
        }
        n.rmutex.release();
      }

      serviceQueue.release();
    } catch (InterruptedException e) {
      e.printStackTrace();
      serviceQueue.release();
    } catch (IOException e) {
      e.printStackTrace();
      unlock(path, exclusive);
      serviceQueue.release();
    } catch (Exception e) {
      unlock(path, exclusive);
      serviceQueue.release();
      throw e;
    }
  }

  @Override
  public void unlock(Path path, boolean exclusive) throws RMIException {
    Node n = getNodeFromPath(tree, path.pathAsList());

    if (n == null) {
      throw new IllegalArgumentException();
    }

    try {
      if (!path.isRoot()) {
        unlock(path.parent(), false);
      }

      if (exclusive) {
        // Invalidate files on SSs different from the one you wrote on
        if (n instanceof Leaf) {
          Leaf l = (Leaf) n;
          List<SS> invalidateSS = new ArrayList<>();
          for (SS ss : l.replicaList) {
            if (!ss.equals(l.most_recent_ss)) {
              ss.cStub.delete(path);
              invalidateSS.add(ss);
            }
          }
          l.replicaList.removeAll(invalidateSS);
        }
        n.resource.release();
      } else {
        n.rmutex.acquire();
        --n.readCount;
        if (n.readCount == 0) {
          n.resource.release();
        }
        n.rmutex.release();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
