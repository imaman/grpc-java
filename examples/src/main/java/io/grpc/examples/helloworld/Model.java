package io.grpc.examples.helloworld;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.base.Objects;

public class Model {
  
  private final List<Map<String, String>> data = new ArrayList<>();
  
  public Model() {
    data.add(newMap("1", "2017-04-28T10:37:05Z", "title_1", "word_2"));
    data.add(newMap("2", "2017-03-28T10:37:05Z", "title_2", "word_1"));
    data.add(newMap("1", "2017-05-28T10:37:05Z", "title_3", "word_3"));
  }

  private Map<String, String> newMap(String userId, String modified, String title, String body) {
    Map<String, String> result = new HashMap<>();
    result.put("user_id", userId);
    result.put("title", title);
    result.put("body", body);
    result.put("last_modified", modified);
    return result;
  }

  public void getPosts(String userId, Consumer<List<Map<String, String>>> callback) {
    List<Map<String, String>> maps = data.stream()
        .filter((Map<String, String> x) -> Objects.equal(userId, x.get("user_id")))
        .collect(Collectors.toList());
    callback.accept(maps);
  }
  public void getTotalPosts(String userId, Consumer<Long> callback) {
    long count = data.stream()
        .filter((Map<String, String> x) -> Objects.equal(userId, x.get("user_id"))).count();
    callback.accept(count);
  }

}
