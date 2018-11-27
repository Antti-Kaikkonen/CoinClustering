const level = require('level');

const db = level('./database', 
{ 
  valueEncoding: 'json', 
  cacheSize: 128*1024*1024, 
  blockSize: 4096, 
  writeBufferSize: 4*1024*1024 
});

db.put("test", "testvalue",  (err) => {
  if (err) return console.log('error1!', err) // some kind of I/O error
  
  db.get("test", (err2, value) => {
    if (err2) return console.log('error2!', err2)
    console.log("value", value);
  });
});