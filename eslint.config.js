import eslintConfigESLint from "eslint-config-eslint";

export default [
  ...eslintConfigESLint,
  {
    ignores: ["node_modules", "coverage", "bake-scripts", "dist"],
  },
  {
    rules: {
      "new-cap": "off",
    },
  },
];
