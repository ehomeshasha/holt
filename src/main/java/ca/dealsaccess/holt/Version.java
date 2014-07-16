package ca.dealsaccess.holt;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import java.io.IOException;

public final class Version {

  private Version() {
  }

  public static String version() {
    return Version.class.getPackage().getImplementationVersion();
  }

  public static String versionFromResource() throws IOException {
    return Resources.toString(Resources.getResource("version"), Charsets.UTF_8);
  }

  public static void main(String[] args) throws IOException {
    System.out.println(version() + ' ' + versionFromResource());
  }
}
