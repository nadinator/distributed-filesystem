package storage;

import common.*;
import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.util.Arrays;
import naming.*;
import rmi.*;

/** Storage server.

    <p>
    Storage servers respond to client file access requests. The files accessible
    through a storage server are those accessible under a given directory of the
    local filesystem.
 */
public class StorageServer implements Storage, Command {

  // Stubs
  Storage storageStub;
  Command commandStub;
  // Skeletons
  Skeleton<Storage> storageSkel;
  Skeleton<Command> commandSkel;
  // The root directory
  File root;
  // Specified ports
  int storage_port = -1;
  int command_port = -1;

  /** Creates a storage server, given a directory on the local filesystem.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
  public StorageServer(File root) {
    if (root == null) {
      throw new NullPointerException();
    }

    this.root = root;
  }

  /** Creates a storage server, given a directory on the local filesystem and ports for the command and storage stubs.

        @param root Directory on the local filesystem. The contents of this
                    directory will be accessible through the storage server.
        @param sp The storage port
        @param cp The command port
        @throws NullPointerException If <code>root</code> is <code>null</code>.
    */
  public StorageServer(File root, int sp, int cp) {
    if (root == null) {
      throw new NullPointerException();
    }

    this.root = root;
    this.storage_port = sp;
    this.command_port = cp;
  }

  /** Starts the storage server and registers it with the given naming
        server.

        @param hostname The externally-routable hostname of the local host on
                        which the storage server is running. This is used to
                        ensure that the stub which is provided to the naming
                        server by the <code>start</code> method carries the
                        externally visible hostname or address of this storage
                        server.
        @param naming_server Remote interface for the naming server with which
                             the storage server is to register.
        @throws UnknownHostException If a stub cannot be created for the storage
                                     server because a valid address has not been
                                     assigned.
        @throws FileNotFoundException If the directory with which the server was
                                      created does not exist or is in fact a
                                      file.
        @throws RMIException If the storage server cannot be started, or if it
                             cannot be registered.
     */
  public synchronized void start(String hostname, Registration naming_server)
    throws RMIException, UnknownHostException, FileNotFoundException {
    // Start the skeletons
    startSkels();

    // Create the stubs for registration
    createStubs();

    // Register SS and delete paths sent by naming server
    reg(naming_server);

    // Prune the files and directories
    prune(root.listFiles());
  }

  /**
   * Starts the skeletons
   * @throws RMIException
   */
  private void startSkels() throws RMIException {
    StorageServer impl = new StorageServer(root);

    if (storage_port != -1) { // Storage port specified
      storageSkel =
        new Skeleton<>(
          Storage.class,
          impl,
          new InetSocketAddress(storage_port)
        );
    } else {
      storageSkel = new Skeleton<>(Storage.class, impl);
    }

    if (command_port != -1) { // Command port specified
      commandSkel =
        new Skeleton<>(
          Command.class,
          impl,
          new InetSocketAddress(command_port)
        );
    } else {
      commandSkel = new Skeleton<>(Command.class, impl);
    }

    try {
      storageSkel.start();
    } catch (RMIException e) {
      System.err.println("Could not start Storage skeleton.");
      throw e;
    }

    try {
      commandSkel.start();
    } catch (RMIException e) {
      System.err.println("Could not start Command skeleton.");
      throw e;
    }
  }

  /**
   * Creates the skeletons
   * @throws RMIException
   * @throws UnknownHostException
   * @throws FileNotFoundException
   */
  private void createStubs()
    throws RMIException, UnknownHostException, FileNotFoundException {
    try {
      storageStub = Stub.create(Storage.class, storageSkel);
    } catch (Exception e) {
      System.err.println("Could not create Storage stub.");
      throw e;
    }

    try {
      commandStub = Stub.create(Command.class, commandSkel);
    } catch (Exception e) {
      System.err.println("Could not create Command stub.");
      throw e;
    }
  }

  /**
   * Registers with the NS and deletes the paths sent back by the naming server
   * @param naming_server
   * @throws RMIException
   * @throws UnknownHostException
   * @throws FileNotFoundException
   */
  private void reg(Registration naming_server)
    throws RMIException, UnknownHostException, FileNotFoundException {
    try {
      Path[] deleteFiles = naming_server.register(
        storageStub,
        commandStub,
        Path.list(root)
      );

      for (Path path : deleteFiles) {
        if (!delete(path)) {
          System.err.println("Couldn't delete " + path);
        }
      }
    } catch (Exception e) {
      System.err.println("Could not register storage server.");
      throw e;
    }
  }

  /**
   * Recursively prunes a directory for any empty files or directories
   * @param dir
   */
  public void prune(File[] dir) {
    for (File d : dir) {
      if (d.isDirectory()) {
        if (d.list().length == 0) { // Delete empty dir
          d.delete();
        } else { // Recursively prune non-empty dir
          prune(d.listFiles());
          if (d.list().length == 0) { // Directory may become empty after pruning
            d.delete();
          }
        }
      }
    }
  }

  /** Stops the storage server.

        <p>
        The server should not be restarted.
     */
  public void stop() {
    Throwable cause = null;

    try {
      storageSkel.stop();
    } catch (Exception e) {
      System.err.println("Couldn't stop Storage skeleton.");
      cause = e;
    }

    try {
      commandSkel.stop();
    } catch (Exception e) {
      System.err.println("Couldn't stop Command skeleton.");
      cause = e;
    }

    stopped(cause);
  }

  /** Called when the storage server has shut down.

        @param cause The cause for the shutdown, if any, or <code>null</code> if
                     the server was shut down by the user's request.
     */
  protected void stopped(Throwable cause) {
    if (cause != null) {
      System.err.println();
      System.err.println("stopped() report: ");
      cause.printStackTrace();
    }
  }

  // The following methods are documented in Storage.java.
  @Override
  public synchronized long size(Path file) throws FileNotFoundException {
    if (file == null) {
      throw new NullPointerException();
    }

    File f = file.toFile(root);

    if (!f.exists() || f.isDirectory()) {
      throw new FileNotFoundException();
    }

    try {
      return Files.size(f.toPath());
    } catch (IOException e) {
      e.printStackTrace();
      return -1;
    }
  }

  @Override
  public synchronized byte[] read(Path file, long offset, int length)
    throws FileNotFoundException, IOException {
    if (file == null) {
      throw new NullPointerException();
    }

    if (offset < 0 || length < 0) {
      throw new IndexOutOfBoundsException();
    }

    File f = file.toFile(root);

    if (!f.exists() || f.isDirectory()) {
      throw new FileNotFoundException();
    }

    if (length + offset > f.length()) {
      throw new IndexOutOfBoundsException();
    }

    try {
      byte[] r = Files.readAllBytes(f.toPath());
      return Arrays.copyOfRange(r, (int) offset, (int) offset + length);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void write(Path file, long offset, byte[] data)
    throws FileNotFoundException, IOException {
    if (file == null || data == null) {
      throw new NullPointerException();
    }

    if (offset < 0) {
      throw new IndexOutOfBoundsException();
    }

    File f = file.toFile(root);

    if (!f.exists() || f.isDirectory()) {
      throw new FileNotFoundException();
    }

    try {
      RandomAccessFile raf = new RandomAccessFile(f, "rw");
      raf.seek(offset);
      raf.write(data);
      raf.close();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  // The following methods are documented in Command.java.
  @Override
  public synchronized boolean create(Path file) {
    if (file.isRoot()) {
      return false;
    }

    File f = file.toFile(root);
    File fp = file.parent().toFile(root);

    if (f.exists()) {
      return false;
    }

    if (!fp.exists()) {
      fp.mkdirs();
    }

    try {
      return f.createNewFile();
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public synchronized boolean delete(Path path) {
    if (path.isRoot()) {
      return false;
    }

    File f = path.toFile(root);

    if (f.isDirectory()) {
      emptyDirectoryContents(f);
    }

    return f.delete();
  }

  /**
   * Deletes all files and directories in a directory
   * @param dir
   */
  private void emptyDirectoryContents(File dir) {
    for (File f : dir.listFiles()) {
      if (f.isDirectory()) { // Recursively empty directory contents
        emptyDirectoryContents(f);
      }
      f.delete();
    }
  }

  /* Project 2 new function */

  @Override
  public boolean copy(Path path, Storage otherServer)
    throws RMIException, FileNotFoundException, IOException {
    if (path == null || otherServer == null) {
      throw new NullPointerException();
    }

    // Weirdness
    if (path.parent().toFile(root).isFile()) {
      delete(path.parent());
    }

    // Delete the file in case it already exists
    delete(path);

    // Create the new file
    if (!create(path)) {
      return false; // File already exists
    }

    // Copy in all its data
    byte[] f_data = otherServer.read(path, 0L, (int) otherServer.size(path));
    write(path, 0L, f_data);
    return true;
  }
}
