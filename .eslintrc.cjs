module.exports = {
  root: true,
  env: {
    es6: true,
    node: true,
  },
  ecmaFeatures: {
    modules: true
  },
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: '2020',
    sourceType: 'module',
  },
  plugins: [ '@typescript-eslint' ],
  extends: [
    'eslint:recommended',
    'plugin:@typescript-eslint/eslint-recommended',
    'plugin:@typescript-eslint/recommended',
  ],
  ignorePatterns: [
    'node_modules',
    'dist',
  ],
  rules: {
    semi: 'off',
    quotes: ['error', 'single'],
    '@typescript-eslint/semi': ['error', 'never'],
    '@typescript-eslint/member-delimiter-style': [
      'error',
      {
        multiline: {
          delimiter: 'none',
          requireLast: true
        },
        singleline: {
          delimiter: 'semi',
          requireLast: false
        }
      }
    ]
  },
}
