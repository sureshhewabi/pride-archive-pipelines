const fs = require('fs');
const readline = require('readline');

const filename = process.argv[2];
const csv_out_file = process.argv[3];
const csv_header = "ProjectAccession,Instrument,Quantification Method,Software,PTM,Species,Publication_ids";

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
        + "," + getAccessions(obj.softwareList) + "," + getAccessions(obj.ptmList) + "," + getSpecies(obj.sample_attributes)
    + "," + getPublicationIds(obj.project_references);
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


function getSpecies(obj) {
    var species = [];
    if (typeof obj === 'undefined') {
        return "";
    }
    obj.forEach(i => {
        if(i.key.accession == "OBI:0100026") {
            i.value.forEach(j => species.push(j.accession))
        }
    });
    return species.join("|");
}

function getPublicationIds(obj) {
    var ids = [];
    if (typeof obj === 'undefined') {
        return "";
    }
    obj.forEach(i => {
        if(i.pubmedID) {
            ids.push("PMID:" + i.pubmedID);
        }
        if(i.doi) {
            ids.push("doi:" + i.doi);
        }
    });
    return ids.join("|");
}
