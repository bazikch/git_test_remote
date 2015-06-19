package f2d

import com.opencsv.CSVReader
import f2d.db.mysql.Utility
import groovy.time.TimeCategory
import groovy.time.TimeDuration
import groovy.sql.Sql

class LabelFall {

	def private source
	def private data

	/**
	 * 
	 * @param filePath
	 */
	def LabelFall(String filePath, Sql sql){
		def start = new Date()
		println ''
		println 'START -> fall label'

		// Filename info
		this.source = filePath

		// CSV reader
		this.data = new CSVReader(new FileReader(filePath))
		String [] nextLine
		def counter = 0
		def noUpdateCounter = 0
		def notPresentSource = []
		nextLine = this.data.readNext() // Skip headers
		while((nextLine = this.data.readNext())!= null){
			println "Recordset $nextLine"
			sql.execute('UPDATE record SET fall = 1 '
					+'WHERE recordset = (SELECT idrecordset FROM recordset WHERE filename = ?) '
					+'AND timestamp BETWEEN ? AND ?', [nextLine[0], nextLine[1], nextLine[2]])
			println "Updated " + sql.getUpdateCount()
			
			if (sql.getUpdateCount() == 0) {
				noUpdateCounter++
				def found = sql.rows('SELECT idrecordset FROM recordset WHERE filename = ?', nextLine[0])
				if(found.size() == 0){
					notPresentSource.add(nextLine[0])
				}
			}
			
			counter++
		}

		def stop = new Date()
		println ''
		println 'Elapse time            : ' + TimeCategory.minus(stop, start)
		println "Treated recordsets     : $counter"
		
		println "Recordsets not updated : $noUpdateCounter"
		println "Missing recordset      : "
		notPresentSource.each {
			println "$it"
		}
		
		println 'END -> fall label'
	}
}