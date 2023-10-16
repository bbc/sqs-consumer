module.exports = {
  default: {
    parallel: 0,
    format: ['html:test/reports/cucumber-report.html'],
    paths: ['test/features'],
    forceExit: true
  }
};
