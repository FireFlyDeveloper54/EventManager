# EventManager

Reflective Event System

## Adding to your project:
If you have not already, add Jitpack as a repository:
```groovy
repositories {
    maven { url 'https://jitpack.io' }
}
```
Add this in the dependencies block:
```groovy
dependencies {
    implementation 'com.github.cubk1:EventManager:-SNAPSHOT'
}
```

## Usage

### Create Instance
```java
EventManager eventManager = new EventManager();
```

### Register
```java
Test test = new Test();
eventManager.register(test); // register
eventManager.unregister(test); // unregister
```

### Call Event
```java
eventManager.call(new UpdateEvent());
```