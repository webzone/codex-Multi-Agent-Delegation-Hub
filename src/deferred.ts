/**
 * Node 20 / ES2022-safe replacement for `Promise.withResolvers()`.
 *
 * `withResolvers` only landed in ES2024 and is absent from the ES2022 lib
 * this project compiles against, so nothing in `src/` or `test/` may use it.
 * Behavior is identical: the promise settles exclusively through the returned
 * `resolve`/`reject`.
 */

export interface Deferred<T> {
  readonly promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
}

export function deferred<T = void>(): Deferred<T> {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
