'use strict';

function isMethod(propertyName, value) {
  return propertyName !== 'constructor' && typeof value === 'function';
}

module.exports = (obj) => {
  const propertyNames = Object.getOwnPropertyNames(obj.constructor.prototype);
  propertyNames.forEach((propertyName) => {
    const value = obj[propertyName];
    if (isMethod(propertyName, value)) {
      obj[propertyName] = value.bind(obj);
    }
  });
};
