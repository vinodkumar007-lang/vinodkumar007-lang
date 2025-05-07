<build>
  <plugins>
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-jar-plugin</artifactId>
      <version>3.3.0</version>
      <configuration>
        <archive>
          <manifest>
            <addClasspath>true</addClasspath>
            <mainClass>com.example.Main</mainClass> <!-- Replace with your actual main class -->
          </manifest>
        </archive>
      </configuration>
    </plugin>
  </plugins>
</build>
