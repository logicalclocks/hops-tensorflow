package io.hops.tensorflow;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

/**
 *
 * 
 */
public class LocalResourceInfo {

  private String name;
  private String path;
  private LocalResourceVisibility visibility;
  private LocalResourceType type;
  //User provided pattern is used if the LocalResource is of type Pattern
  private String pattern;

  public LocalResourceInfo(String name, String path, LocalResourceVisibility visibility, LocalResourceType type,
      String pattern) {
    this.name = name;
    this.path = path;
    this.visibility = visibility;
    this.type = type;
    this.pattern = pattern;
  }

  public LocalResourceInfo(String name, String path, LocalResourceVisibility visibility, LocalResourceType type) {
    this.name = name;
    this.path = path;
    this.visibility = visibility;
    this.type = type;
  }

  public LocalResourceInfo(String name, String path, String visibility, String type) {
    this.name = name;
    this.path = path;
    this.type = LocalResourceType.valueOf(type.toUpperCase());
    this.visibility = LocalResourceVisibility.valueOf(visibility.toUpperCase());
  }

  public LocalResourceInfo(String name, String path, String visibility, String type, String pattern) {
    this.name = name;
    this.path = path;
    this.type = LocalResourceType.valueOf(type.toUpperCase());
    this.visibility = LocalResourceVisibility.valueOf(visibility.toUpperCase());
    this.pattern = pattern;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public LocalResourceVisibility getVisibility() {
    return visibility;
  }

  public void setVisibility(LocalResourceVisibility visibility) {
    this.visibility = visibility;
  }

  public LocalResourceType getType() {
    return type;
  }

  public void setType(LocalResourceType type) {
    this.type = type;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  @Override
  public String toString() {
    return "LocalResourceDTO{" + "name=" + name + ", path=" + path
        + ", visibility=" + visibility + ", type=" + type + ", pattern="
        + pattern + '}';
  }

}
