package rmi;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/** RMI skeleton

    <p>
    A skeleton encapsulates a multithreaded TCP server. The server's clients are
    intended to be RMI stubs created using the <code>Stub</code> class.

    <p>
    The skeleton class is parametrized by a type variable. This type variable
    should be instantiated with an interface. The skeleton will accept from the
    stub requests for calls to the methods of this interface. It will then
    forward those requests to an object. The object is specified when the
    skeleton is constructed, and must implement the remote interface. Each
    method in the interface should be marked as throwing
    <code>RMIException</code>, in addition to any other exceptions that the user
    desires.

    <p>
    Exceptions may occur at the top level in the listening and service threads.
    The skeleton's response to these exceptions can be customized by deriving
    a class from <code>Skeleton</code> and overriding <code>listen_error</code>
    or <code>service_error</code>.
*/
public class Skeleton<T> {

  InetSocketAddress address; // Can't use -ve port in constructor
  ServerSocket listenSocket;
  Class<T> intf;
  T impl;
  boolean isRunning = false; // true iff Skeleton.start() is called
  Thread listenThread;
  int port = -1;

  /**
   * Address getter
   */
  public InetSocketAddress getAddress() {
    return address;
  }

  /**
   * isRunning getter (state)
   */
  public boolean getIsRunning() {
    return isRunning;
  }

  public int getPort() {
    return port;
  }

  /**
   * Address setter
   * @param addr The address to be set to skeleton
   */
  public void setAddress(InetSocketAddress addr) {
    this.address = addr;
  }

  /** Creates a <code>Skeleton</code> with no initial server address. The
        address will be determined by the system when <code>start</code> is
        called. Equivalent to using <code>Skeleton(null)</code>.

        <p>
        This constructor is for skeletons that will not be used for
        bootstrapping RMI - those that therefore do not require a well-known
        port.

        @param c (intf) An object representing the class of the interface for which the
                 skeleton server is to handle method call requests.
        @param server (impl) An object implementing said interface. Requests for method
                      calls are forwarded by the skeleton to this object.
        @throws Error If <code>c</code> does not represent a remote interface -
                      an interface whose methods are all marked as throwing
                      <code>RMIException</code>.
        @throws NullPointerException If either of <code>c</code> or
                                     <code>server</code> is <code>null</code>.
     */
  public Skeleton(Class<T> c, T server) {
    // Null check
    if (c == null || server == null) {
      throw new NullPointerException("Arguments cannot be null!");
    }
    // Remote interface check
    if (!checkRemote(c)) {
      throw new Error("Interface is not remote!");
    }
    // Assigning variables
    this.intf = c;
    this.impl = server;
  }

  /** Creates a <code>Skeleton</code> with the given initial server address.

        <p>
        This constructor should be used when the port number is significant.

        @param c An object representing the class of the interface for which the
                 skeleton server is to handle method call requests.
        @param server An object implementing said interface. Requests for method
                      calls are forwarded by the skeleton to this object.
        @param address The address at which the skeleton is to run. If
                       <code>null</code>, the address will be chosen by the
                       system when <code>start</code> is called.
        @throws Error If <code>c</code> does not represent a remote interface -
                      an interface whose methods are all marked as throwing
                      <code>RMIException</code>.
        @throws NullPointerException If either of <code>c</code> or
                                     <code>server</code> is <code>null</code>.
     */
  public Skeleton(Class<T> c, T server, InetSocketAddress address) {
    // Null check
    if (c == null || server == null) {
      throw new NullPointerException("Arguments cannot be null!");
    }
    // Remote interface check
    if (!checkRemote(c)) {
      throw new Error("Interface is not remote!");
    }
    // Assigning variables
    this.intf = c;
    this.impl = server;
    this.address = address;
  }

  /** Called when the listening thread exits.

        <p>
        The listening thread may exit due to a top-level exception, or due to a
        call to <code>stop</code>.

        <p>
        When this method is called, the calling thread owns the lock on the
        <code>Skeleton</code> object. Care must be taken to avoid deadlocks when
        calling <code>start</code> or <code>stop</code> from different threads
        during this call.

        <p>
        The default implementation does nothing.

        @param cause The exception that stopped the skeleton, or
                     <code>null</code> if the skeleton stopped normally.
     */
  protected void stopped(Throwable cause) {}

  /** Called when an exception occurs at the top level in the listening
        thread.

        <p>
        The intent of this method is to allow the user to report exceptions in
        the listening thread to another thread, by a mechanism of the user's
        choosing. The user may also ignore the exceptions. The default
        implementation simply stops the server. The user should not use this
        method to stop the skeleton. The exception will again be provided as the
        argument to <code>stopped</code>, which will be called later.

        @param exception The exception that occurred.
        @return <code>true</code> if the server is to resume accepting
                connections, <code>false</code> if the server is to shut down.
     */
  protected boolean listen_error(Exception exception) {
    return false;
  }

  /** Called when an exception occurs at the top level in a service thread.

        <p>
        The default implementation does nothing.

        @param exception The exception that occurred.
     */
  protected void service_error(RMIException exception) {}

  /** Starts the skeleton server.

        <p>
        A thread is created to listen for connection requests, and the method
        returns immediately. Additional threads are created when connections are
        accepted. The network address used for the server is determined by which
        constructor was used to create the <code>Skeleton</code> object.

        @throws RMIException When the listening socket cannot be created or
                             bound, when the listening thread cannot be created,
                             or when the server has already been started and has
                             not since stopped.
     */
  public synchronized void start() throws RMIException {
    // Start listening socket
    try {
      isRunning = true;
      listenSocket = new ServerSocket();
      listenSocket.bind(address);
      // Reassign address
      address = (InetSocketAddress) listenSocket.getLocalSocketAddress();
      port = address.getPort(); // update it so you know it's been initialized
    } catch (IOException e) {}

    // Start listening thread
    listenThread =
      new Thread(
        new Runnable() {
          @Override
          public void run() {
            Socket clientSocket;

            while (isRunning) {
              try {
                clientSocket = listenSocket.accept(); // listening for conn's
                new Thread(new ServiceThread(clientSocket)).start();
              } catch (IOException e) {}
            }
          }
        }
      );

    listenThread.start();
  }

  /** Stops the skeleton server, if it is already running.

        <p>
        The listening thread terminates. Threads created to service connections
        may continue running until their invocations of the <code>service</code>
        method return. The server stops at some later time; the method
        <code>stopped</code> is called at that point. The server may then be
        restarted.
     */
  public synchronized void stop() {
    // Close listening socket
    // Stop the listening thread
    // Call the stopped() function
    // Check if listenSocket
    if (
      this.isRunning == true && listenSocket != null && listenThread != null
    ) {
      this.isRunning = false;
      try {
        listenSocket.close();
        listenThread.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    stopped(null);
  }

  /** Checks if a class is remote (all methods throw RMIException)  */
  public boolean checkRemote(Class<T> c) {
    boolean remoteCheck = true; // overall-class RMI check
    boolean methodCheck = false; // method-specific RMI check

    // Loop through the mthods in class c
    for (Method m : c.getMethods()) {
      // Loop through the exceptions thrown by each method
      Class<?>[] extypes = m.getExceptionTypes();
      for (Class<?> clazz : extypes) {
        if (clazz == RMIException.class) {
          methodCheck = true;
        }
      }
      // If one of the methods does not throw RMIException, we have failed the check
      if (methodCheck == false) {
        remoteCheck = false;
      }
      // Reset methodCheck
      methodCheck = false;
    }

    return remoteCheck;
  }

  /** A Runnable class that is invoked upoed proxy request-arrival.
   * See start() for usage.
   */
  public class ServiceThread implements Runnable {

    Socket clientSocket;
    ObjectInputStream in;
    ObjectOutputStream out;

    // You pass in the socket upon ServiceThread construction
    ServiceThread(Socket cs) {
      this.clientSocket = cs;
      try {
        this.in = new ObjectInputStream(cs.getInputStream());
        this.out = new ObjectOutputStream(cs.getOutputStream());
      } catch (Exception e) {}
    }

    /** in.readObject() wrapper
     * @param errmesg The message you want displayed in case of error
     */
    public Object ReadObject(String errmesg) {
      Object retval = null;

      try {
        retval = in.readObject();
      } catch (Exception e) {}

      return retval;
    }

    @Override
    /** The main logic that runs when a new thread is started to service a client request */
    public void run() {
      Object result = null;
      Method m;

      try {
        // Read received method parameters
        String methodName = (String) ReadObject("Couldn't read method name!");
        Class<?>[] argTypes = (Class<?>[]) ReadObject(
          "Coulnd't read argument types!"
        );
        Object[] args = (Object[]) ReadObject("Couldn't read arguments!");

        try {
          // Get the requested method and invoke it
          m = intf.getDeclaredMethod(methodName, argTypes);
          result = m.invoke(impl, args);
        } catch (Exception e) {
          result = e;
        }
      } catch (Exception e) {
        result = e;
      } finally {
        try {
          // Write back the results to Stub
          out.writeObject(result);
          out.flush();
          in.close();
          out.close();
          if (clientSocket != null) {
            clientSocket.close();
          }
        } catch (Exception e) {}
      }
    }
  }
}
