var path = require("path");
var exec = require('child_process').exec;

console.log(process.argv);


var p = path.resolve("vendor", "bin", "phpunit");
var phpunit = exec(`${p} tests --verbose  ${process.argv.slice(2).join(' ')}`);
phpunit.stdout.pipe(process.stdout);
phpunit.stderr.pipe(process.stderr);