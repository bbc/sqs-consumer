module.exports = {
  preset: 'ts-jest',
  transform: {
    '<rootDir>/test': ['ts-jest', { isolatedModules: true }]
  },
  testEnvironment: 'node'
};
