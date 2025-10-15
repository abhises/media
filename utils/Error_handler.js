// utils/ErrorHandler.js

export class ValidationError extends Error {
  constructor(message, data = null) {
    super(message);
    this.name = "ValidationError";
    this.data = data;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class ConflictError extends Error {
  constructor(message, data = null) {
    super(message);
    this.name = "ConflictError";
    this.data = data;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class NotFoundError extends Error {
  constructor(message, data = null) {
    super(message);
    this.name = "NotFoundError";
    this.data = data;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class StateTransitionError extends Error {
  constructor(message, data = null) {
    super(message);
    this.name = "StateTransitionError";
    this.data = data;
    Error.captureStackTrace(this, this.constructor);
  }
}
