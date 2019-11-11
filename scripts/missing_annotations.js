const fs = require('fs');
const readline = require('readline');

const filename = process.argv[2];
const csv_out_file = process.argv[3];
const csv_header = "ProjectAccession,Instrument,Quantification Method,Software,PTM";

var csv_writer = fs.createWriteStream(csv_out_file, {
});
csv_writer.write(csv_header + "\n");

const readInterface = readline.createInterface({
    input: fs.createReadStream(filename),
    console: false
});

readInterface.on('line', function (line) {
    var obj = JSON.parse(line);
    var csv_line = obj.accession + "," + getAccessions(obj.instruments) + "," + getAccessions(obj.quantificationMethods)
        + "," + getAccessions(obj.softwareList) + "," + getAccessions(obj.ptmList);
    csv_writer.write(csv_line + "\n");
});

function getAccessions(obj) {
    var accessions = [];
    if (typeof obj === 'undefined') {
        return "";
    }
    obj.forEach(i => accessions.push(i.accession));
    return accessions.join("|");
}