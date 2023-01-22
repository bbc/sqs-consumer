function delay(ms) {
  return new Promise((res) => setTimeout(res, ms));
}

exports.delay = delay;
