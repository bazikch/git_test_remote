package f2d

import com.opencsv.*
import groovy.time.TimeCategory
import groovy.time.TimeDuration
import groovy.sql.Sql

class LoadRecordSet {
	def private source
	def private data
	def private sql
	def private categoryCode
	def private fallDirection
	def private fallDirectionCode = [avant:'AV', arriere: 'AR',gauche:'G',droite:'D',autre:'A',bras:'B',asseoirLever:'AL',course:'C',escalierD:'ED',escalierM:'EM',marche:'M']
	def private subjectAge
	def private subjectSexe
	def private subjectCode
	def private auxiliaryMeansCode
	def private time
	def private date
	def private filename

	/**
	 * Extract information from filename.
	 */
	def private extractFileNameDetails(){
		def fileParams = this.source.split('_')

		this.categoryCode = fileParams[0][-2..-1]
		this.fallDirection = fallDirectionCode.get(fileParams[1])
		this.subjectAge = fileParams[3][-2..-1].toInteger()
		this.subjectSexe = fileParams[3][0]
		this.subjectCode = fileParams[4]
		this.auxiliaryMeansCode = fileParams[5]
		this.time = fileParams[6]

		def Date fileDate = Date.parse('dd-mm-yyyy',fileParams[7][0..(fileParams[7].size()-5)])
		this.date = fileDate.format('yyyy-mm-dd')

		this.filename = new File(this.source).getName().split ('\\.')[0]
	}

	/**
	 * Recordset creation.
	 *
	 * @return recordset identifier
	 */
	def private createRecordSet(){
		def subject = this.createSubject(this.subjectCode, this.subjectAge, this.subjectSexe, 0)
		def category = this.getCategory()
		def auxiliaryMeans = this.getAuxiliaryMeans()

		def existingRecord = this.sql.rows('SELECT idrecordset FROM recordset WHERE filename = ?', this.filename)

		if(existingRecord.size() == 0){
			def params = [this.date, this.time, this.filename, subject, category, auxiliaryMeans, 0, 0, 0, 0]
			def newRecord = this.sql.executeInsert('INSERT INTO recordset (date, time, filename, subject, category, auxiliary_means, duration_ms, empty_linear_acceleration_x, empty_linear_acceleration_y, empty_linear_acceleration_z) VALUES (?,?,?,?,?,?,?,?,?,?)', params)
			return newRecord[0][0]
		}

		return existingRecord.idrecordset[0]
	}

	/**
	 * Create subject if not already present.
	 * Return subject's identifier.
	 *
	 * @param code
	 * @param age
	 * @param sexe
	 * @param weight
	 * @return subject identifier
	 */
	def private createSubject(String code, int age, String sexe, float weight){
		def params = [code, age, sexe, weight]
		def existingRecord = this.sql.rows('SELECT idsubject FROM subject WHERE code = ? AND age = ? AND sexe = ? AND weight = ?', params)

		if(existingRecord.size() == 0){
			println "Subject $params needs to be created."
			existingRecord = this.sql.executeInsert('INSERT INTO subject (code, age, sexe, weight) VALUES (?, ?, ?, ?)', params)
			return existingRecord[0][0]
		}
		println "Subject $existingRecord.idsubject used."

		return existingRecord.idsubject[0]
	}

	/**
	 * Get auxilary means identifier.
	 *
	 * @return auxiliary means identifier
	 */
	def private getAuxiliaryMeans (){
		def auxiliaryMeans = this.sql.rows('SELECT idauxiliary_means FROM auxiliary_means WHERE code = ?', this.auxiliaryMeansCode)
		println "Auxiliary means $auxiliaryMeans.idauxiliary_means used."
		return auxiliaryMeans.idauxiliary_means[0]
	}

	/**
	 * Get category identifier.
	 *
	 * @return category identifier
	 */
	def private getCategory(){
		def code = this.categoryCode + this.fallDirection
		def category = this.sql.rows('SELECT idcategory FROM category WHERE code = ?', code)
		println "Category $category.idcategory used."
		return category.idcategory[0]
	}

	/**
	 *
	 * @param recordSetId
	 * @return
	 */
	def private insertRecordSetDetails(String [] line, int recordSetId){
		this.sql.execute('INSERT INTO record '
				+'(timestamp,linear_acceleration_x,linear_acceleration_y,linear_acceleration_z,fall,recordset) VALUES '
				+'(?,?,?,?,?,?)',
				[
					Double.parseDouble(line[0]),
					Float.parseFloat((line[1].isEmpty()) ? '0' : line[1]),
					Float.parseFloat((line[2].isEmpty()) ? '0' : line[2]),
					Float.parseFloat((line[3].isEmpty()) ? '0' : line[3]),
					0,
					recordSetId
				])
	}

	/**
	 * Constructor.
	 * 
	 * @param filePath
	 */
	def LoadRecordSet(String filePath, Sql sql){
		def start = new Date()
		println ''
		println '-----------------------------------'
		println "START -> recordset $filePath"
		println ''

		// Filename info
		this.source = filePath
		extractFileNameDetails()

		// Sql
		this.sql = sql

		//Avoid loading a recordset already present
		def existingRecord = sql.rows('SELECT idrecordset FROM recordset WHERE filename = ?', this.filename)
		def counter = 0
		if(existingRecord.size() == 0){

			// Record set
			int recordId = this.createRecordSet()
			println "Recordset $recordId used."

			// CSV reader
			this.data = new CSVReader(new FileReader(filePath))
			String [] nextLine
			nextLine = this.data.readNext() // Skip headers
			while((nextLine = this.data.readNext())!= null){
				this.insertRecordSetDetails(nextLine, recordId)
				counter++
			}
		} else {
			println "Recordset $filePath already loaded!"
		}

		def stop = new Date()
		println ''
		println 'Elapsed time    : ' + TimeCategory.minus(stop, start)
		println "Inserted records: $counter"
		println "END -> recordset $filePath"
	}
}
