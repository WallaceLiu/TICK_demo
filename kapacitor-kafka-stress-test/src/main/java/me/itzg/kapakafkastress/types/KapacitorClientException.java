package me.itzg.kapakafkastress.types;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

public class KapacitorClientException extends ResponseStatusException {

  public KapacitorClientException(String message) {
    super(HttpStatus.BAD_REQUEST, message);
  }
}
