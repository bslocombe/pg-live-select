var _ = require('lodash')
var randomString  = require('random-strings')

var querySequence = require('../helpers/querySequence')

var indexes = {
  students: [ ],
  assignments: [ 'class_id' ],
  scores: [ 'id ASC', 'score DESC', 'assignment_id', 'student_id' ]
}

/**
 * Generate data structure describing a random scores set
 * @param Integer classCount        total number of classes to generate
 * @param Integer assignPerClass    number of assignments per class
 * @param Integer studentsPerClass  number of students enrolled in each class
 * @param Integer classesPerStudent number of classes each student is enrolled
 */
exports.generate =
function(classCount, assignPerClass, studentsPerClass, classesPerStudent) {
  var studentCount = Math.ceil(classCount / classesPerStudent) * studentsPerClass
  var assignCount  = classCount * assignPerClass
  var scoreCount   = assignCount * studentsPerClass

  var students = _.range(studentCount).map(index => {
    return {
      id   : index + 1,
      name : randomString.alphaLower(10)
    }
  })

  var assignments = _.range(assignCount).map(index => {
    return {
      id       : index + 1,
      class_id : (index % classCount) + 1,
      name     : randomString.alphaLower(10),
      value    : Math.ceil(Math.random() * 100)
    }
  })

  var scores = _.range(scoreCount).map(index => {
    var assignId = Math.floor(index / studentsPerClass) + 1
    var baseStudent =
      Math.floor((assignments[assignId - 1].class_id - 1) / classesPerStudent)

    return {
      id            : index + 1,
      assignment_id : assignId,
      student_id    : (baseStudent * studentsPerClass) +
                      (index % studentsPerClass) + 1,
      score         : Math.ceil(Math.random() * assignments[assignId - 1].value)
    }
  })

  return { assignments, students, scores }
}


function columnTypeFromName(name, mode) {
  switch(mode) {
    case 'pg':
      switch(name){
        case 'id'       : return 'serial NOT NULL'
        case 'name'     : return 'character varying(50) NOT NULL'
        case 'big_name' : return 'text NOT NULL'
        default         : return 'integer NOT NULL'
      }
      break;
    case 'my':
      switch(name){
        case 'id'       : return 'INT NOT NULL AUTO_INCREMENT'
        case 'name'     : return 'VARCHAR(50) NOT NULL'
        case 'big_name' : return 'TEXT NOT NULL'
        default         : return 'INT NOT NULL'
      }
      break;
  }
}

/**
 * Create/replace test tables filled with fixture data
 * @param  Object   generation     Output from generate() function above
 * @return Promise
 */
exports.install = function(generation) {
  var mode = process.env.MODE;

  return Promise.all(_.map(generation, (rows, table) => {

    var primaryKeySnippet, analyzeCommand;
    switch(mode) {
      case 'pg':
        primaryKeySnippet = `CONSTRAINT ${table}_pkey PRIMARY KEY (id)`;
        analyzeCommand = `ANALYZE `;
        break;
      case 'my':
        primaryKeySnippet = `PRIMARY KEY (id)`;
        analyzeCommand = `ANALYZE TABLE `;
        break;
    }

    // Create tables, Insert data
    var installQueries = [
      `DROP TABLE IF EXISTS ${table} CASCADE`,

      `CREATE TABLE ${table} (
        ${_.keys(rows[0])
          .map(column => `${column} ${columnTypeFromName(column, mode)}`).join(', ')},
        ${primaryKeySnippet})`
    ]

    // Insert max 500 rows per query
    installQueries = installQueries.concat(_.chunk(rows, 500).map(rowShard => {
      var valueCount = 0
      return [`INSERT INTO ${table}
          (${_.keys(rowShard[0]).join(', ')})
         VALUES ${rowShard.map(row =>
          `(${_.map(row, () => '$' + ++valueCount).join(', ')})`
          ).join(', ')}`,
       _.flatten(rowShard.map(row => _.values(row))) ]
    }))

    // Related: test/variousQueries.es6 :: applyTableSuffixes
    // Suffixes allow concurrent test running
    var tablePrefix = table.split('_')[0]

    if(indexes[tablePrefix] && indexes[tablePrefix].length !== 0) {
      for(let index of indexes[tablePrefix]) {
        installQueries.push(`
          CREATE INDEX
            ${table}_index_${index.split(' ')[0]}
          ON ${table} (${index})`)
      }
    }

    installQueries.push(analyzeCommand + table)

    return querySequence(installQueries)
  }))

}

