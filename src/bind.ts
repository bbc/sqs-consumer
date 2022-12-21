/**
 * Determines if the property is a method
 * @param propertyName the name of the property
 * @param value the value of the property
 */
function isMethod(propertyName: string, value: any): boolean {
  return propertyName !== 'constructor' && typeof value === 'function';
}

/**
 * Auto binds the provided properties
 * @param obj an object containing the available properties
 */
export function autoBind(obj: object): void {
  const propertyNames = Object.getOwnPropertyNames(obj.constructor.prototype);
  propertyNames.forEach((propertyName) => {
    const value = obj[propertyName];
    if (isMethod(propertyName, value)) {
      obj[propertyName] = value.bind(obj);
    }
  });
}
