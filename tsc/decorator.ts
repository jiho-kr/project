function pushSafe(object, arrayName, element) {
  const array = object[arrayName] = object[arrayName] || [];
  array.push(element);
}

function endpoint(method: 'get' | 'put' | 'post' | 'del', uri: string) {
  return (target, _name, desc: PropertyDescriptor) => {
    const handler = desc.value;
    const constructor = target.constructor;
    pushSafe(constructor, 'endpoints', { method, uri, handler });
  };
}

export function get(uri: string) {
  return endpoint('get', uri);
}

export function put(uri: string) {
  return endpoint('put', uri);
}

export function post(uri: string) {
  return endpoint('post', uri);
}

export function del(uri: string) {
  return endpoint('del', uri);
}
