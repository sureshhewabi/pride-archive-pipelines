const fs = require('fs');
const readline = require('readline');

const filename = process.argv[2];
const csv_out_file = process.argv[3];
const githubCsvFile = process.argv[4];
const mode = process.argv[5];
const csv_header = "ProjectAccession,Instrument,Quantification Method,Software,PTM,Species,Publication_ids";

const csv_writer = fs.createWriteStream(csv_out_file, {});

var incrementalFlg = false;
if (mode == "--incremental" || mode == "-i") {
    incrementalFlg = true;
}

const readInterface = readline.createInterface({
    input: fs.createReadStream(filename),
    console: false
});

var githubAccessions = [];

function parseMongoExportFile() {
    readInterface.on('line', function (line) {
        let obj = JSON.parse(line);
        const accession = obj.accession;
        if((!incrementalFlg) || (incrementalFlg && !githubAccessions.includes(accession))) {
            let csv_line = accession + "," + getAccessions(obj.instruments) + "," + getAccessions(obj.quantificationMethods)
                + "," + getAccessions(obj.softwareList) + "," + getAccessions(obj.ptmList) + "," + getSpecies(obj.sample_attributes)
                + "," + getPublicationIds(obj.project_references);
            csv_writer.write(csv_line + "\n");
        }
    });
}

if (incrementalFlg) {
    const githubFileReader = readline.createInterface({
        input: fs.createReadStream(githubCsvFile),
        console: false
    });
    githubFileReader.on('line', function (line) {
        githubAccessions.push(line.split(",")[0]);
        csv_writer.write(line + "\n");
    });
    githubFileReader.on('close', function() {
        parseMongoExportFile()
    });
} else {
    csv_writer.write(csv_header + "\n");
    parseMongoExportFile();
}

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
        if (i.key.accession == "OBI:0100026") {
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
        if (i.pubmedID) {
            ids.push("PMID:" + i.pubmedID);
        }
        if (i.doi) {
            ids.push("doi:" + i.doi);
        }
    });
    return ids.join("|");
}
