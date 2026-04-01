const findFreePort = require('find-free-port');

module.exports = async function getFreePort(startPort = 8000, endPort = 8100) {
  return new Promise((resolve, reject) => {
    findFreePort(startPort, endPort, '127.0.0.1', (err, port) => {
      if (err) reject(err);
      else resolve(port);
    });
  });
};