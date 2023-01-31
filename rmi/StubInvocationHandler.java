package rmi;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;

/** Invocation handler for the Stub class */
public class StubInvocationHandler implements InvocationHandler, Serializable {

  public InetSocketAddress skelAddr; // Address of the skeleton to connect to
  public Class<?> intf; // the interface implemented by the stub

  public StubInvocationHandler(InetSocketAddress skelAddr, Class<?> intf) {
    this.skelAddr = skelAddr;
    this.intf = intf;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable {
    Object result = null; // invocation result

    try {
      String mName = method.getName();
      Class<?>[] mParamTypes = method.getParameterTypes();

      // Check if method is local
      switch (mName) {
        case "toString":
          result = toString();
          break;
        case "equals":
          result = equals(args[0]);
          break;
        case "hashCode":
          result = hashCode();
          break;
        default:
          result = invokeCall(mName, mParamTypes, args);
          break;
      }
    } catch (Exception e) {
      throw e;
    }

    return result;
  }

  /** Method for invoking remote calls
   * @param name method name
   * @param types method parameter types
   * @param args method arguments
   * @return the result of the invocation
   * @throws Throwable
   * @throws IOException if sockets fail
   * @throws ClassNotFoundException if reading in object fails
   */
  public Object invokeCall(String name, Class<?>[] types, Object[] args)
    throws Throwable, RMIException {
    // Invocation result
    Object result = null;
    // Socket and streams.
    Socket socket = null;
    ObjectOutputStream out = null;
    ObjectInputStream in = null;

    try {
      // Get streams
      socket = new Socket();
      socket.connect(skelAddr); // connect to skeleton
      out = new ObjectOutputStream(socket.getOutputStream());
      in = new ObjectInputStream(socket.getInputStream()); // <- Error happens here????

      // Write method parameters to Skeleton
      out.writeObject(name);
      out.writeObject(types);
      out.writeObject(args);
      out.flush();

      // Receive invokation results
      result = in.readObject();
    } catch (Exception e) {
      throw new RMIException(e);
    } finally {
      try {
        // Close everything when done
        if (socket != null && out != null && in != null) {
          out.close();
          in.close();
          socket.close(); // do you really need to close the conn after one invocation?
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    // Throw exception if that was the result
    if (result instanceof Exception) {
      throw ((Exception) result).getCause();
    }

    return result;
  }

  @Override
  public String toString() {
    return getClass().getName() + '@' + Integer.toHexString(hashCode());
  }

  @Override
  public boolean equals(Object o) {
    // Null check
    if (this == o) {
      return true;
    }
    // Null check (when this is not null)
    if (o == null) {
      return false;
    }
    // Class check
    try {
      if (Proxy.getInvocationHandler(o).getClass() != this.getClass()) {
        return false;
      }
    } catch (IllegalArgumentException e) {
      return false;
    }
    // Parameters check
    return (
      Objects.equals(
        intf,
        ((StubInvocationHandler) Proxy.getInvocationHandler(o)).intf
      ) &&
      Objects.equals(
        skelAddr,
        ((StubInvocationHandler) Proxy.getInvocationHandler(o)).skelAddr
      )
    );
  }

  @Override
  public int hashCode() {
    return intf.hashCode() + skelAddr.hashCode();
  }
}
