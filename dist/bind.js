"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.autoBind = void 0;
function isMethod(propertyName, value) {
    return propertyName !== 'constructor' && typeof value === 'function';
}
function autoBind(obj) {
    const propertyNames = Object.getOwnPropertyNames(obj.constructor.prototype);
    propertyNames.forEach((propertyName) => {
        const value = obj[propertyName];
        if (isMethod(propertyName, value)) {
            obj[propertyName] = value.bind(obj);
        }
    });
}
exports.autoBind = autoBind;
