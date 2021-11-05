package libinpirncs

import (
	"archive/zip"
	"bytes"
	"encoding/xml"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type sourceFile struct {
	name   string
	reader io.ReadCloser
}

// ParseBilan transforme un fichier en variable de type Bilan
func ParseBilan(bilanByte []byte, reference string) (Bilan, error) {
	v := XMLBilans{}
	xml.Unmarshal(bilanByte, &v)

	var bilan Bilan
	bilan.Reference = reference
	bilan.Adresse = v.Bilan.Identite.Adresse
	bilan.Siren = v.Bilan.Identite.Siren
	dateClotureExercice, err := time.Parse("20060102", v.Bilan.Identite.DateClotureExercice)
	if err != nil {
		bilan.Report = append(bilan.Report, "dateClotureExercice: "+err.Error())
	}
	bilan.DateClotureExercice = dateClotureExercice
	bilan.CodeGreffe = v.Bilan.Identite.CodeGreffe
	bilan.NumDepot = v.Bilan.Identite.NumDepot
	bilan.NumGestion = v.Bilan.Identite.NumGestion
	bilan.CodeActivite = v.Bilan.Identite.CodeActivite
	dateClotureExercicePrecedent, err := time.Parse("20060102", v.Bilan.Identite.DateClotureExercicePrecedent)
	if err != nil {
		bilan.Report = append(bilan.Report, "dateClotureExercicePrecedent: "+err.Error())
	}
	bilan.DateClotureExercicePrecedent = dateClotureExercicePrecedent
	bilan.DureeExercice = v.Bilan.Identite.DureeExercice
	bilan.DureeExercicePrecedent = v.Bilan.Identite.DureeExercicePrecedent
	dateDepot, err := time.Parse("20060102", v.Bilan.Identite.DateDepot)
	if err != nil {
		bilan.Report = append(bilan.Report, "dateDepot: "+err.Error())
	}
	bilan.DateDepot = dateDepot
	bilan.CodeMotif = v.Bilan.Identite.CodeMotif
	bilan.CodeTypeBilan = v.Bilan.Identite.CodeTypeBilan
	bilan.CodeDevise = v.Bilan.Identite.CodeDevise
	bilan.CodeOrigineDevise = v.Bilan.Identite.CodeOrigineDevise
	bilan.CodeConfidentialite = v.Bilan.Identite.CodeConfidentialite
	bilan.Denomination = v.Bilan.Identite.Denomination
	bilan.InfoTraitement = v.Bilan.Identite.InfoTraitement
	bilan.XMLSource = string(bilanByte)
	bilan.Lignes = make(map[string]*int)
	for _, page := range v.Bilan.Detail.Page {
		for _, liasse := range page.Liasse {
			key := Key{v.Bilan.Identite.CodeTypeBilan, liasse.Code}
			s, err := GetSchema(key)
			if err != nil {
				bilan.Report = append(bilan.Report, "Code liasse non identifié: "+key.CodePoste)
			}
			if err == nil {
				if s[0] != "" && liasse.M1 != nil {
					bilan.Lignes[s[0]] = liasse.M1
				}
				if s[1] != "" && liasse.M2 != nil {
					bilan.Lignes[s[1]] = liasse.M2
				}
				if s[2] != "" && liasse.M3 != nil {
					bilan.Lignes[s[2]] = liasse.M3
				}
				if s[3] != "" && liasse.M4 != nil {
					bilan.Lignes[s[3]] = liasse.M4
				}
			}
		}
	}
	return bilan, nil
}

func deepUnzip(zipFileReader sourceFile) chan sourceFile {
	unzipChannel := make(chan sourceFile)
	go func() {
		l := len(zipFileReader.name)
		if zipFileReader.name[l-3:l] == "zip" {
			buffer := new(bytes.Buffer)
			buffer.ReadFrom(zipFileReader.reader)
			zipFileReader.reader.Close()
			bytesFile := buffer.Bytes()
			bufferSize := int64(len(bytesFile))
			zipFiles, err := zip.NewReader(bytes.NewReader(bytesFile), bufferSize)
			if err != nil {
				return
			}
			for _, z := range zipFiles.File {
				l := len(z.Name)
				innerReader, err := z.Open()
				if err != nil {
					continue
				}
				if z.Name[l-3:l] == "zip" {
					innerChannel := deepUnzip(sourceFile{z.Name, innerReader})
					for r := range innerChannel {
						unzipChannel <- r
					}
				} else {
					unzipChannel <- sourceFile{z.Name, innerReader}
				}
			}
		} else {
			unzipChannel <- zipFileReader
		}
		close(unzipChannel)
	}()
	return unzipChannel
}

// BilanWorker produit tous les bilans contenus dans une arborescence
// l'exploration de l'arborescence se fait avec une profondeur arbitraire et traite également les zip imbriqués
func BilanWorker(basePath string) chan Bilan {
	channel := make(chan Bilan)

	files, err := ioutil.ReadDir(basePath)
	if err != nil {
		log.Print(basePath + ": probleme d'accès, passe")
		close(channel)
		return channel
	}

	go func() {
		for _, file := range files {
			if file.IsDir() {
				childrenChannel := BilanWorker(basePath + "/" + file.Name())
				for bilan := range childrenChannel {
					channel <- bilan
				}
			} else {
				file, _ := os.Open(basePath + "/" + file.Name())
				unzipChannel := deepUnzip(sourceFile{file.Name(), file})
				for z := range unzipChannel {
					l := len(z.name)
					if z.name[l-3:l] == "xml" || z.name[l-3:l] == "XML" {
						bufferFile := new(bytes.Buffer)
						bufferFile.ReadFrom(z.reader)
						z.reader.Close()
						bilanBytes := bufferFile.Bytes()
						bilan, _ := ParseBilan(bilanBytes, file.Name()+" > "+z.name)
						channel <- bilan
					}
				}
				file.Close()
			}
		}
		close(channel)
	}()
	return channel
}
