package main

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sony/sonyflake"
	"github.com/xuri/excelize/v2"
)

func main() {
	// db := createDatabase()
	dbFname := "db/data.db"
	db, _ := sql.Open("sqlite3", dbFname)
	defer db.Close()

	// sf := sonyflake.NewSonyflake(sonyflake.Settings{})
	// createTableStatusK1(db)
	// importStatusK1(db, sf)
	// importUnesa(db, sf)
	// importUpi(db, sf)
	// importUndiksa(db, sf)
	// importUnivGorontalo(db, sf)
	// importUNS(db, sf)
	// importUnm(db, sf)
	// importUad(db, sf)
	// importUnjem(db, sf)
	// importUnbeng(db, sf)
	// importAlmuslim(db, sf)
	exportXlsx(db, "Universitas Al Muslim")
	// vacuumTableRekening(db)
}

type colHeader struct {
	cols    map[string]int
	reverse map[string]string
	maps    map[string]string
	_names  []string
}

var sqlInsertRekening = `INSERT INTO rekening (
			id,
			simpkb_id,
			nik,
			_nama,
			_lptk,
			_bank,
			_rekening,
			_nominal,
			_status
		) VALUES (
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?
		)`

func (h *colHeader) scan(xl *excelize.File, sheetName string, rowNum int) {
	if h.reverse != nil {
		return
	}
	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()

	// skip header
	rowNum-- //to rowIdx
	for i := 0; i < rowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}
	if !rows.Next() {
		fmt.Println("baris kosong!")
		os.Exit(0)
	}
	//retval := make(map[string]int)
	cols, err := rows.Columns()
	if err != nil {
		exit(err)
	}

	reSpaces, err := regexp.Compile(`\s`)
	if err != nil {
		exit(err)
	}
	reNonAlphaNum, err := regexp.Compile(`[^a-z0-9_]`)
	if err != nil {
		exit(err)
	}

	h.cols = make(map[string]int)
	h.reverse = make(map[string]string)
	h.maps = make(map[string]string)

	for i, name := range cols {
		origName := name
		if strings.TrimSpace(origName)=="" {
			fmt.Printf("nama kolom blank\n")
			os.Exit(0)
		}
		if _, ok := h.reverse[origName]; ok {
			fmt.Printf("nama kolom tidak unik: %s !\n", origName)
			os.Exit(0)
		}
		name = strings.ToLower(name)
		name = reSpaces.ReplaceAllString(name, "_")
		name = reNonAlphaNum.ReplaceAllString(name, "")
		h.cols[name] = i
		h.reverse[origName] = name
		h.maps[name] = origName
	}
	h._names = make([]string, len(h.reverse))
	for i, val := range h.cols {
		h._names[val] = i
	}
}

func (h *colHeader) setNames(names ...string) {
	for _, name := range names {
		if _, ok := h.cols[name]; !ok {
			fmt.Printf("name tidak tersedia: %s !\n", name)
			os.Exit(0)
		}
	}
	h._names = names
}

func (h colHeader) printNames() {
	for _, name := range h.names() {
		fmt.Printf("\t\"%s\",\n", name)
	}
	fmt.Print("\n\n")
}

func (h colHeader) printSqlCreate() {
	var sb strings.Builder
	sb.WriteString("\tid BIGINT PRIMARY KEY NOT NULL,\n")
	for _, val := range h.names() {
		sb.WriteString("\t" + val + " TEXT NOT NULL,\n")
	}
	fmt.Println(strings.TrimSuffix(sb.String(), ",\n") + "\n")
}

func (h colHeader) printSqlInsert() {
	cols := make([]string, len(h._names))
	for i, val := range h.names() {
		cols[i] = "\t\t" + val
	}
	colsStr := "\n" + strings.TrimSuffix("\t\tid,\n"+strings.Join(cols, ",\n"), ",\n") + "\n"
	placeHolders := "\n" + strings.TrimSuffix(strings.Repeat("\t\t?,\n", len(h._names)+1), ",\n") + "\n"
	fmt.Printf("\tINSERT INTO %%s (%s\t) VALUES (%s\t)\n\n", colsStr, placeHolders)
}

func (h colHeader) names() []string {
	if h._names == nil {
		fmt.Println("scan header terlebih dahulu!")
		os.Exit(0)
	}
	return h._names
}

func (h colHeader) values(id uint64, col []string) []any {
	retval := make([]any, len(h._names)+1)
	if len(col) < len(h._names) {
		for j := len(col); j <= len(h._names); j++ {
			col = append(col, "")
		}
	}
	retval[0] = id
	for i, name := range h.names() {
		retval[i+1] = col[h.cols[name]]
	}
	return retval
}

func importPks(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/pks.xlsx"
	// sheetName := "Sheet1"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 1
	dataStartRowNum := 2

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	header.setNames(
		"id_simpkb",
		"nuptk",
		"nama_lengkap",
		"tempat_lahir",
		"tanggal_lahir",
		"kelamin",
		"angkatan",
		"lptk",
		"bidang_studi_ppg",
		"tahap",
		"akses_modul_epks",
		"validasi_surel",
		"setuju_epks",
		"validasi_kyc",
		"ttd_epks",
		"ttd_laporan",
		"selesai",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	tblName := "pks"
	sqlInsertPks := fmt.Sprintf(`INSERT INTO %s (
			id,
			id_simpkb,
			nuptk,
			nama_lengkap,
			tempat_lahir,
			tanggal_lahir,
			kelamin,
			angkatan,
			lptk,
			bidang_studi_ppg,
			tahap,
			akses_modul_epks,
			validasi_surel,
			setuju_epks,
			validasi_kyc,
			ttd_epks,
			ttd_laporan,
			selesai
	) VALUES (
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?,
			?
	)`, tblName)

	stmt, err := db.Prepare(sqlInsertPks)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		if _, err = stmt.Exec(header.values(id, col)...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func importStatusK1(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/status-k1.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := "Sheet1"
	headerRowNum := 1
	dataStartRowNum := 2

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	header.setNames(
        "nama_lengkap",
        "tempat_lahir",
        "tanggal_lahir",
        "kelamin",
        "no_hp",
        "no_ukg",
        "nik",
        "nip",
        "nuptk",
        "nama_sekolah",
        "npsn_sekolah",
        "jenjang_sekolah",
        "jabatan_fungsional",
        "nim",
        "lptk",
        "bidang_studi_ppg",
        "nama_kelas",
        "status_lapor_diri",
        "alasan_lapor_diri",
        "sumber_pembiayaan",
        "status_tambahan",
        "status",
        "alamat_rumah",
        "kotakabupaten",
        "provinsi",
        "kode_pos",
        "instansi_asal",
        "alamat_instansi_asal",
        "kotakabupaten_instansi_asal",
        "provinsi_instansi_asal",
        "kode_pos_instansi_asal",
        "npwp",
        "nama_sesuai_rekening",
        "nama_bank",
        "bank_cabang",
        "nomor_rekening",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	tblName := "status_lapor_diri_k1"
	sqlInsertPks := fmt.Sprintf(` INSERT INTO %s (
		id,
		nama_lengkap,
		tempat_lahir,
		tanggal_lahir,
		kelamin,
		no_hp,
		no_ukg,
		nik,
		nip,
		nuptk,
		nama_sekolah,
		npsn_sekolah,
		jenjang_sekolah,
		jabatan_fungsional,
		nim,
		lptk,
		bidang_studi_ppg,
		nama_kelas,
		status_lapor_diri,
		alasan_lapor_diri,
		sumber_pembiayaan,
		status_tambahan,
		status,
		alamat_rumah,
		kotakabupaten,
		provinsi,
		kode_pos,
		instansi_asal,
		alamat_instansi_asal,
		kotakabupaten_instansi_asal,
		provinsi_instansi_asal,
		kode_pos_instansi_asal,
		npwp,
		nama_sesuai_rekening,
		nama_bank,
		bank_cabang,
		nomor_rekening
) VALUES (
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?
)`, tblName)

	stmt, err := db.Prepare(sqlInsertPks)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		if _, err = stmt.Exec(header.values(id, col)...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func importUnesa(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/Universitas Negeri Surabaya.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	for _, sheet := range xl.WorkBook.Sheets.Sheet {
		sheetName := sheet.Name
		headerRowNum := 4
		dataStartRowNum := 6

		header := colHeader{}
		header.scan(xl, sheetName, headerRowNum)
		if header.names()[0] != "no" { // <=============== warning
			fmt.Println("invalid header")
			os.Exit(0)
		}
		header.setNames(
			"nomor_ukg",
			"nomor_ktp",
			"nama_penerima_bantuan_pemerintah",
			"nama_universitas",
			"nama_bank",
			"nomor_rekening",
			"jumlah_bantuan_pemerintah_rp",
		)

		// header.printSqlCreate()
		// header.printSqlInsert()
		// header.printNames()

		rows, err := xl.Rows(sheetName)
		if err != nil {
			exit(err)
		}
		defer rows.Close()
		// skip header
		dataStartRowNum-- //to rowIdx
		for i := 0; i < dataStartRowNum; i++ {
			if !rows.Next() {
				fmt.Println("baris kosong!")
				os.Exit(0)
			}
		}

		stmt, err := db.Prepare(sqlInsertRekening)
		if err != nil {
			exit(err)
		}

		fmt.Printf("mulai proses baris sheet %s...\n", sheetName)
		for rows.Next() {
			col, err := rows.Columns()
			fmt.Println(col)
			if err != nil {
				exit(err)
			}
			//profiling kolom no ukg posisi = 3 dari 0
			if len(col) < len(header._names) {
				fmt.Println("break on empty rows")
				break
			}
			var id uint64
			if id, err = sf.NextID(); err != nil {
				exit(err)
			}

			args := header.values(id, col)
			args = append(args, "") // missing status hack

			if _, err = stmt.Exec(args...); err != nil {
				exit(err)
			}

		}
		fmt.Printf("selesai proses baris sheet: %s\n", sheetName)
	}
}

func importUpi(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/UNIVERSITAS PENDIDIKAN INDONESIA.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 1
	dataStartRowNum := 2

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	if header.names()[0] != "no" { // <=============== warning
		fmt.Println("invalid header")
		os.Exit(0)
	}
	// "nomor_ukg",
	// "nomor_ktp",
	// "nama_penerima_bantuan_pemerintah",
	// "nama_universitas",
	// "nama_bank",
	// "nomor_rekening",
	// "jumlah_bantuan_pemerintah_rp",
	header.setNames(
		"nomor_ukg",
		"nomor_ktp",
		"nama_penerima_bantuan_pemerintah",
		"nama_universitas",
		"nama_bank",
		"nomor_rekening",
		"jumlah_bantuan_pemerintah",
		"status_lapor_diri",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	stmt, err := db.Prepare(sqlInsertRekening)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		//profiling kolom no ukg posisi = 3 dari 0
		if len(col) < len(header._names) {
			fmt.Println("break on empty rows")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		if _, err = stmt.Exec(header.values(id, col)...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func importUnivGorontalo(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/UNIVERSITAS NEGERI GORONTALO.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 1
	dataStartRowNum := 2

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	if header.names()[0] != "no" { // <=============== warning
		fmt.Println("invalid header")
		os.Exit(0)
	}
	// "nomor_ukg",
	// "nomor_ktp",
	// "nama_penerima_bantuan_pemerintah",
	// "nama_universitas",
	// "nama_bank",
	// "nomor_rekening",
	// "jumlah_bantuan_pemerintah_rp",
	header.setNames(
		"nomor_ukg",
		"nomor_ktp",
		"nama_penerima_bantuan_pemerintah",
		"nama_universitas",
		"nama_bank",
		"nomor_rekening",
		"jumlah_bantuan_pemerintah_rp",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	stmt, err := db.Prepare(sqlInsertRekening)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		//profiling kolom no ukg posisi = 3 dari 0
		if len(col) < len(header._names) {
			fmt.Println("break on empty rows")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		args := header.values(id, col) // hack
		args = append(args, "")

		if _, err = stmt.Exec(args...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func importUndiksa(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/Universitas Pendidikan Ganesha.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 3
	dataStartRowNum := 4

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	if header.names()[0] != "no" { // <=============== warning
		fmt.Println("invalid header")
		os.Exit(0)
	}
	// "nomor_ukg",
	// "nomor_ktp",
	// "nama_penerima_bantuan_pemerintah",
	// "nama_universitas",
	// "nama_bank",
	// "nomor_rekening",
	// "jumlah_bantuan_pemerintah_rp",
	header.setNames(
		"no_ukg",
		"nik",
		"nama_lengkap",
		"lptk",
		"nama_bank",
		"nomor_rekening",
		"jml_bantuan_pemerintah_rp",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	stmt, err := db.Prepare(sqlInsertRekening)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		//profiling kolom no ukg posisi = 3 dari 0
		if len(col) < len(header._names) {
			fmt.Println("break on empty rows")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		args := header.values(id, col) // hack
		args = append(args, "")

		if _, err = stmt.Exec(args...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func importUnm(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/Universitas Negeri Malang.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 4
	dataStartRowNum := 6

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	if header.names()[0] != "no" { // <=============== warning
		fmt.Println("invalid header")
		os.Exit(0)
	}
	// "nomor_ukg",
	// "nomor_ktp",
	// "nama_penerima_bantuan_pemerintah",
	// "nama_universitas",
	// "nama_bank",
	// "nomor_rekening",
	// "jumlah_bantuan_pemerintah_rp",
	header.setNames(
        "nomor_ukg",
        "nomor_ktp",
        "nama_penerima_bantuan_pemerintah",
        "nama_universitas",
        "nama_bank",
        "nomor_rekening__va",
        "jumlah_bantuan_pemerintah",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	stmt, err := db.Prepare(sqlInsertRekening)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		//profiling kolom no ukg posisi = 3 dari 0
		if len(col) < len(header._names) {
			fmt.Println("break on empty rows")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		args := header.values(id, col) // hack
		args = append(args, "")

		if _, err = stmt.Exec(args...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func importUad(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/Universitas Ahmad Dahlan.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 1
	dataStartRowNum := 2

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	if header.names()[0] != "no" { // <=============== warning
		fmt.Println("invalid header")
		os.Exit(0)
	}
	// "nomor_ukg",
	// "nomor_ktp",
	// "nama_penerima_bantuan_pemerintah",
	// "nama_universitas",
	// "nama_bank",
	// "nomor_rekening",
	// "jumlah_bantuan_pemerintah_rp",
	header.setNames(
        "no_ukg",
        "no_ktp",
		"nama_penerima_bantuan_pemerintah",
        "nama_universitas",
        "nama_bank",
        "nomor_rekening_virtual_account",
        "jumlah_bantuan_pemerintah_rp",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	stmt, err := db.Prepare(sqlInsertRekening)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		//profiling kolom no ukg posisi = 3 dari 0
		if len(col) < len(header._names) {
			fmt.Println("break on empty rows")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		args := header.values(id, col) // hack
		args = append(args, "")

		if _, err = stmt.Exec(args...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func importUnjem(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/UNIVERSITAS JEMBER.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 2
	dataStartRowNum := 3

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	if header.names()[0] != "no" { // <=============== warning
		fmt.Println("invalid header")
		os.Exit(0)
	}
	// "nomor_ukg",
	// "nomor_ktp",
	// "nama_penerima_bantuan_pemerintah",
	// "nama_universitas",
	// "nama_bank",
	// "nomor_rekening",
	// "jumlah_bantuan_pemerintah_rp",
	header.setNames(
        "no_ukg",
        "nik",
        "nama_penerima_banpem",
        "nama_universitas",
        "nama_bank",
        "no_rekening",
        "jumlah_banpem_rp",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	stmt, err := db.Prepare(sqlInsertRekening)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		//profiling kolom no ukg posisi = 3 dari 0
		if len(col) < len(header._names) {
			fmt.Println("break on empty rows")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		args := header.values(id, col) // hack
		args = append(args, "")

		if _, err = stmt.Exec(args...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func importUnbeng(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/Universitas Bengkulu.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 3
	dataStartRowNum := 4

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	if header.names()[0] != "no" { // <=============== warning
		fmt.Println("invalid header")
		os.Exit(0)
	}
	// "nomor_ukg",
	// "nomor_ktp",
	// "nama_penerima_bantuan_pemerintah",
	// "nama_universitas",
	// "nama_bank",
	// "nomor_rekening",
	// "jumlah_bantuan_pemerintah_rp",
	header.setNames(
        "no_ukg",
        "no_ktp",
        "nama_penerima_bantuan",
        "nama_universitas",
        "nama_bank",
        "no_virtual_account",
        "jumlah_bantuan_pemerintah__rp",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	stmt, err := db.Prepare(sqlInsertRekening)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		//profiling kolom no ukg posisi = 3 dari 0
		if len(col) < len(header._names) {
			fmt.Println("break on empty rows")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		args := header.values(id, col) // hack
		args = append(args, "")

		if _, err = stmt.Exec(args...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func importAlmuslim(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/Universitas Al Muslim.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 1
	dataStartRowNum := 2

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	if header.names()[0] != "no" { // <=============== warning
		fmt.Println("invalid header")
		os.Exit(0)
	}
	// "nomor_ukg",
	// "nomor_ktp",
	// "nama_penerima_bantuan_pemerintah",
	// "nama_universitas",
	// "nama_bank",
	// "nomor_rekening",
	// "jumlah_bantuan_pemerintah_rp",
	header.setNames(
        "nomor_ukg",
        "nomor_ktp",
        "nama_penerima_bantuan_pemerintah",
        "nama_universitas",
        "nama_bank",
        "nomor_rekening",
        "jumlah_bantuan_pemerintah_rp",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	stmt, err := db.Prepare(sqlInsertRekening)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		//profiling kolom no ukg posisi = 3 dari 0
		if len(col) < len(header._names) {
			fmt.Println("break on empty rows")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		args := header.values(id, col) // hack
		args = append(args, "")

		if _, err = stmt.Exec(args...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

//===

func importUNS(db *sql.DB, sf *sonyflake.Sonyflake) {
	xlFname := "input/Universitas Sebelas Maret.xlsx"

	_, err := os.Stat(xlFname)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println(`file excel "input.xlsx" tidak ditemukan!`)
		}
	}

	xl, err := excelize.OpenFile(xlFname)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer func() {
		if err := xl.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	sheetName := xl.WorkBook.Sheets.Sheet[0].Name
	headerRowNum := 1
	dataStartRowNum := 2

	header := colHeader{}
	header.scan(xl, sheetName, headerRowNum)
	if header.names()[0] != "no" { // <=============== warning
		fmt.Println("invalid header")
		os.Exit(0)
	}
	// "nomor_ukg",
	// "nomor_ktp",
	// "nama_penerima_bantuan_pemerintah",
	// "nama_universitas",
	// "nama_bank",
	// "nomor_rekening",
	// "jumlah_bantuan_pemerintah_rp",
	header.setNames(
        "ukg",
        "nama",
        "bank",
        "va",
	)

	// header.printSqlCreate()
	// header.printSqlInsert()
	// header.printNames()

	rows, err := xl.Rows(sheetName)
	if err != nil {
		exit(err)
	}
	defer rows.Close()
	// skip header
	dataStartRowNum-- //to rowIdx
	for i := 0; i < dataStartRowNum; i++ {
		if !rows.Next() {
			fmt.Println("baris kosong!")
			os.Exit(0)
		}
	}

	sqlInsertRekening := `INSERT INTO rekening (
		id,
		simpkb_id,
		nik,
		_nama,
		_lptk,
		_bank,
		_rekening,
		_nominal,
		_status
	) VALUES (
		?,
		?,
		"",
		?,
		"Universitas Sebelas Maret",
		?,
		?,
		0,
		?
	)`

	stmt, err := db.Prepare(sqlInsertRekening)
	if err != nil {
		exit(err)
	}

	fmt.Println("mulai proses baris...")
	for rows.Next() {
		col, err := rows.Columns()
		fmt.Println(col)
		if err != nil {
			exit(err)
		}
		//profiling kolom no ukg posisi = 3 dari 0
		if len(col) < len(header._names) {
			fmt.Println("break on empty rows")
			break
		}
		var id uint64
		if id, err = sf.NextID(); err != nil {
			exit(err)
		}

		args := header.values(id, col) // hack
		args = append(args, "")

		if _, err = stmt.Exec(args...); err != nil {
			exit(err)
		}

	}
	fmt.Println("selesai proses baris")
}

func exportXlsx(db *sql.DB, lptk string) {

	colTitle := map[string]string{
		"id":               "ID",
		"simpkb_id":        "SIMPKB ID",
		"id_simpkb":        "SIMPKB ID PKS",
		"nik":              "NIK",
		"_nama":            "Nama",
		"_lptk":            "LPTK",
		"_bank":            "Nama Bank",
		"_rekening":        "No Rekening",
		"_nominal":         "Jumlah",
		"akses_modul_epks": "Tgl Akses E-PKS",
		"status": "Status Lapor Diri",
		"konfirmasi": "Mengundurkan Diri (Ya/Tidak)",
	}

	rows, err := db.Query(`SELECT
		a.id,a.simpkb_id,b.id_simpkb,a.nik,a._nama,a._lptk,a._bank,a._rekening,a._nominal,b.akses_modul_epks,c.status, '' AS konfirmasi
		FROM rekening a
		LEFT JOIN pks b ON a.simpkb_id=b.id_simpkb
		LEFT JOIN status_lapor_diri_k1 c ON a.simpkb_id=c.no_ukg
		WHERE a._lptk=?`,
	lptk)
	if err != nil {
		exit(err)
	}
	defer func() {
		errRow := rows.Close()
		if errRow != nil {
			fmt.Println(errRow.Error())
		}
	}()

	cols, err := rows.Columns()
	if err != nil {
		exit(err)
	}

	resPtr := make([]any, len(cols))
	for i := 0; i < len(cols); i++ {
		resPtr[i] = new(sql.NullString)
	}

	results := []map[string]any{}

	for rows.Next() {
		if err = rows.Scan(
			resPtr...,
		); err != nil {
			exit(err)
		}
		result := make(map[string]any)
		for i := 0; i < len(cols); i++ {
			val := *resPtr[i].(*sql.NullString)
			if val.Valid {
				result[cols[i]] = val.String
			} else {
				result[cols[i]] = "NULL"
			}

			// val := resPtr[i].(*sql.NullString)
			// if (*val).Valid {
			// 	result[cols[i]] = (*val).String
			// } else {
			// 	result[cols[i]] = "NULL"
			// }
		}
		results = append(results, result)
	}

	xl := excelize.NewFile()

	firstColName, err := excelize.ColumnNumberToName(1)
	if err != nil {
		exit(err)
	}
	lastColName, err := excelize.ColumnNumberToName(len(cols))
	if err != nil {
		exit(err)
	}

	styleBold, err := xl.NewStyle(&excelize.Style{
		Font: &excelize.Font{
			Bold: true,
		},
	})
	if err != nil {
		exit(err)
	}

	idx := xl.NewSheet("input")
	currentRow := int(1)
	for i, val := range cols {
		colName, err := excelize.ColumnNumberToName(i + 1)
		if err != nil {
			exit(err)
		}
		xl.SetCellValue("input", colName+strconv.Itoa(currentRow), colTitle[val])
	}
	styleHighlightYellow, err := xl.NewStyle(&excelize.Style{
		Fill: excelize.Fill{
			Type:    "pattern",
			Pattern: 1,
			Color:   []string{"#ffff00"},
		},
	})
	if err != nil {
		exit(err)
	}
	styleHighlightRed, err := xl.NewStyle(&excelize.Style{
		Fill: excelize.Fill{
			Type:    "pattern",
			Pattern: 1,
			Color:   []string{"#ff0000"},
		},
	})
	if err != nil {
		exit(err)
	}
	// customNumFmt := "#,##0.00"
	// styleCurrency, err := xl.NewStyle(&excelize.Style{
	// 	CustomNumFmt: &customNumFmt,
	// })
	// if err != nil {
	// 	exit(err)
	// }

	currentRow++
	for _, result := range results {
		fmt.Println(result)
		firstColAddr := firstColName + strconv.Itoa(currentRow)
		lastColAddr := lastColName + strconv.Itoa(currentRow)
		for i, col := range cols {
			name, err := excelize.ColumnNumberToName(i + 1)
			if err != nil {
				exit(err)
			}
			curColAddr := name + strconv.Itoa(currentRow)
			switch col {
			case "_nominal":
				num, err := strconv.ParseUint(result[col].(string), 10, 64)
				if err != nil {
					exit(err)
				}
				result[col] = num
			case "konfirmasi":
				// xl.SetCellStyle("input", curColAddr, curColAddr, styleHighlightYellow)
				// xl.SetCellStyle("input", curColAddr, curColAddr, styleCurrency) // overrided!
			}
			xl.SetCellValue("input", curColAddr, result[col])
		}
		if result["id_simpkb"].(string) == "NULL" || strings.TrimSpace(result["akses_modul_epks"].(string)) == "" {
			xl.SetCellStyle("input", firstColAddr, lastColAddr, styleHighlightYellow)
		}
		if strings.TrimSpace(result["status"].(string)) != "REG" && result["id_simpkb"].(string) != "NULL" {
			xl.SetCellStyle("input", firstColAddr, lastColAddr, styleHighlightRed)
		}
		currentRow++
	}

	xl.SetCellStyle("input", firstColName+strconv.Itoa(1), lastColName+strconv.Itoa(1), styleBold)
	xl.SetColVisible("input", "A", false)

	xlAutoWidth(xl, "input")

	// Set active sheet of the workbook.
	xl.SetActiveSheet(idx)
	xl.DeleteSheet("Sheet1")

	// Save spreadsheet by the given path.
	if err := xl.SaveAs(fmt.Sprintf("output/%s.xlsx", lptk)); err != nil {
		exit(err)
	}
}

func xlAutoWidth(xl *excelize.File, sheetName string) {
	cs, err := xl.GetCols(sheetName)
	if err != nil {
		exit(err)
	}
	for i, c := range cs {
		largestWidth := 0
		for _, rowCell := range c {
			cellWidth := utf8.RuneCountInString(rowCell) + 2
			if cellWidth > largestWidth {
				largestWidth = cellWidth
			}
		}
		name, err := excelize.ColumnNumberToName(i + 1)
		if err != nil {
			exit(err)
		}
		xl.SetColWidth(sheetName, name, name, float64(largestWidth))
	}
}

func createDatabase() *sql.DB {
	dbFname := "db/data.db"
	os.Remove(dbFname)
	fmt.Printf("membuat database %s...\n", dbFname)
	file, err := os.Create(dbFname)
	if err != nil {
		exit(err)
	}
	file.Close()
	fmt.Printf("database \"%s\" berhasil dibuat\n", dbFname)
	outputDb, _ := sql.Open("sqlite3", dbFname)
	//defer outputDb.Close()
	createTablePks(outputDb)
	createTableRekening(outputDb)
	return outputDb
}

func createTablePks(db *sql.DB) {
	tblName := "pks"
	createTableSQL := fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT PRIMARY KEY NOT NULL,
        id_simpkb TEXT NOT NULL,
        nuptk TEXT NOT NULL,
        nama_lengkap TEXT NOT NULL,
        tempat_lahir TEXT NOT NULL,
        tanggal_lahir TEXT NOT NULL,
        kelamin TEXT NOT NULL,
        angkatan TEXT NOT NULL,
        lptk TEXT NOT NULL,
        bidang_studi_ppg TEXT NOT NULL,
        tahap TEXT NOT NULL,
        akses_modul_epks TEXT NOT NULL,
        validasi_surel TEXT NOT NULL,
        setuju_epks TEXT NOT NULL,
        validasi_kyc TEXT NOT NULL,
        ttd_epks TEXT NOT NULL,
        ttd_laporan TEXT NOT NULL,
        selesai TEXT NOT NULL
	  );`, tblName)

	fmt.Printf("membuat tabel %s...\n", tblName)
	statement, err := db.Prepare(createTableSQL)
	if err != nil {
		exit(err)
	}
	statement.Exec()
	fmt.Printf("tabel %s berhasil dibuat\n", tblName)
}

func createTableStatusK1(db *sql.DB) {
	tblName := "status_lapor_diri_k1"
	createTableSQL := fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT PRIMARY KEY NOT NULL,
        nama_lengkap TEXT NOT NULL,
        tempat_lahir TEXT NOT NULL,
        tanggal_lahir TEXT NOT NULL,
        kelamin TEXT NOT NULL,
        no_hp TEXT NOT NULL,
        no_ukg TEXT NOT NULL,
        nik TEXT NOT NULL,
        nip TEXT NOT NULL,
        nuptk TEXT NOT NULL,
        nama_sekolah TEXT NOT NULL,
        npsn_sekolah TEXT NOT NULL,
        jenjang_sekolah TEXT NOT NULL,
        jabatan_fungsional TEXT NOT NULL,
        nim TEXT NOT NULL,
        lptk TEXT NOT NULL,
        bidang_studi_ppg TEXT NOT NULL,
        nama_kelas TEXT NOT NULL,
        status_lapor_diri TEXT NOT NULL,
        alasan_lapor_diri TEXT NOT NULL,
        sumber_pembiayaan TEXT NOT NULL,
        status_tambahan TEXT NOT NULL,
        status TEXT NOT NULL,
        alamat_rumah TEXT NOT NULL,
        kotakabupaten TEXT NOT NULL,
        provinsi TEXT NOT NULL,
        kode_pos TEXT NOT NULL,
        instansi_asal TEXT NOT NULL,
        alamat_instansi_asal TEXT NOT NULL,
        kotakabupaten_instansi_asal TEXT NOT NULL,
        provinsi_instansi_asal TEXT NOT NULL,
        kode_pos_instansi_asal TEXT NOT NULL,
        npwp TEXT NOT NULL,
        nama_sesuai_rekening TEXT NOT NULL,
        nama_bank TEXT NOT NULL,
        bank_cabang TEXT NOT NULL,
        nomor_rekening TEXT NOT NULL
	  );`, tblName)

	fmt.Printf("membuat tabel %s...\n", tblName)
	statement, err := db.Prepare(createTableSQL)
	if err != nil {
		exit(err)
	}
	statement.Exec()
	fmt.Printf("tabel %s berhasil dibuat\n", tblName)
}

func createTableRekening(db *sql.DB) {
	tblName := "rekening"
	createTableSQL := fmt.Sprintf(`CREATE TABLE %s (
		id        BIGINT          PRIMARY KEY
				NOT NULL
				UNIQUE,
		simpkb_id TEXT            UNIQUE
				NOT NULL,
		nik                       NOT NULL,
		_nama     TEXT            NOT NULL,
		_lptk     TEXT            NOT NULL,
		_bank     TEXT            NOT NULL
				DEFAULT "",
		_rekening TEXT            NOT NULL
				DEFAULT "",
		_nominal  NUMERIC (10, 2) NOT NULL
				DEFAULT (0),
		_status   TEXT            DEFAULT ""
	  );`, tblName)

	fmt.Printf("membuat tabel %s...\n", tblName)
	stmt, err := db.Prepare(createTableSQL)
	if err != nil {
		exit(err)
	}
	stmt.Exec()
	fmt.Printf("tabel %s berhasil dibuat\n", tblName)
}

func vacuumTableRekening(db *sql.DB) {
	tblName := "rekening"
	strSQL := fmt.Sprintf(`DELETE FROM %s; VACUUM;`, tblName)

	fmt.Printf("mengosongkan tabel %s...\n", tblName)
	stmt, err := db.Prepare(strSQL)
	if err != nil {
		exit(err)
	}
	stmt.Exec()
	fmt.Printf("tabel %s berhasil dikosongkan\n", tblName)
}

func exit(err error) {
	fmt.Println(err.Error())
	os.Exit(0)
}
