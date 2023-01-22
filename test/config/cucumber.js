module.exports = {
  default: {
    parallel: 1,
    format: ['html:test/reports/cucumber-report.html'],
    publishQuiet: true,
    paths: ['test/features'],
  }
}