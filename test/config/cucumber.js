module.exports = {
  default: {
    parallel: 0,
    format: ['html:test/reports/cucumber-report.html'],
    publishQuiet: true,
    paths: ['test/features'],
    forceExit: true
  }
};
