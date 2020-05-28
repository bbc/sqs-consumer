function isMethod(propertyName: string, value: any): boolean {
  return propertyName !== 'constructor' && typeof value === 'function';
}

export function autoBind(obj: object): void {
  const propertyNames = Object.getOwnPropertyNames(obj.constructor.prototype);
  propertyNames.forEach((propertyName) => {
    const value = obj[propertyName];
    if (isMethod(propertyName, value)) {
      obj[propertyName] = value.bind(obj);
    }
  });
}
