/*
ingests the ~29 gigabytes of courts data contained to a mongodb database
*/
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"log"
	"time"
	"context"
	"encoding/csv"
	"io"
	"strconv"
	"errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strings"
	"bufio"

)

func main() {
	
	un, pwd, host, _, _, prot, cert := os.Getenv("USERNAME"), os.Getenv("PASSWORD"), os.Getenv("HOST"), os.Getenv("PORT"), os.Getenv("DATABASE"), os.Getenv("PROTOCOL"), os.Getenv("CERT") 
	connecstring := prot + "://" + un + ":"  + pwd + "@" + host + "?authSource=admin&replicaSet=simons-database&tls=true&tlsCAFile=" + cert
	client, err := mongo.NewClient(options.Client().ApplyURI(connecstring))
	if err != nil {
		log.Fatal(err)
	}
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	scotusReference, err := readScotusReference("./data/scdb_matchup_2020-01-16.csv")
	if err != nil {
		log.Println(err)
	}
	scotusCaseReference, err := getNewScotus(scotusReference, "./data/SCDB_2020_01_justiceCentered_Citation.csv")
	if err != nil {
		log.Println(err)
	}
	
	scotusCaseReference, err = getOldScotus(scotusReference, "./data/SCDB_Legacy_06_justiceCentered_Citation.csv", scotusCaseReference) 
	if err != nil {
		log.Println(err)
	} 
	circuitCaseReference, err := getOldCircuit("./data/cta96.csv")
	if err != nil {
		log.Println(err)
	}

	circuitCaseReference, err = getNewCircuit("./data/cta02.csv", circuitCaseReference)
	if err != nil {
		log.Println(err)
	} 
	
	circuitJudgeReference, scotusJudgeReference, err := getCircuitJudge("./data/appct_judges.csv", "./data/justicesdata2021.csv")
	if err != nil {
		log.Println(err)
	} 

	districtCaseReference, err := getDistrict("./data/fdcdata.csv")
	if err != nil {
		log.Println(err)
	}

	scotusCaseReference, circuitCaseReference, districtCaseReference  = attachOpinions("./data/us_text_20200604/data/data.jsonl", scotusJudgeReference, scotusCaseReference, circuitJudgeReference, circuitCaseReference, districtCaseReference)
	
	sendScotus(scotusCaseReference, client)
	sendCircuit(circuitCaseReference, client)
	sendDistrict(districtCaseReference, client)

}

func readScotusReference(path string)  (map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	n := 0
	errMsg := "failed to process line(s): "
	scotusReference := make(map[string]string)
	for {
		line, error := csvReader.Read()
		if error == io.EOF {
			break
		} else if error != nil {
			errMsg += ( strconv.Itoa(n) + ", ")
		}
		if line[0] == "strong" {
			scotusReference[line[5]] = line[9]
		}
		n++
	}

	if errMsg == "failed to process line(s): " {
		return scotusReference, errors.New(errMsg) 
	}
	return scotusReference, nil
}

func getNewScotus(scotusReference map[string]string, path string) (map[string]map[string]interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	n := 0
	courtReference := make(map[string]map[string]interface{})
	currCase := ""
	lines, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	for _, line := range lines {
		header := lines[0]
		
		if _, caseExists := scotusReference[line[0]]; !(caseExists) {
	//		fmt.Println("case not found: " + line[0])
			//os.Exit(1)
			continue 
		}
		newCurrCase := line[0]

		if currCase != newCurrCase {
			courtReference[scotusReference[line[0]]] = make(map[string]interface{})
			courtReference[scotusReference[line[0]]]["votes"] = make(map[int]map[string]interface{})
			for i, v := range line[:53] {
				if v == "" {
					courtReference[scotusReference[line[0]]][header[i]] = nil
				} else {
					switch i {
					case 3:
						strarr := strings.Split(v, "-")
						var intarray[6]int64
						for is, vs := range strarr {
							intarray[is], err = strconv.ParseInt(vs, 10, 64)
							if err != nil {
								log.Fatal(err)
							}
						}
						courtReference[scotusReference[line[0]]]["voteId"] = intarray
					case 4,15,16:
						tme,err := time.Parse("1/2/2006", v)
						if err != nil {
							log.Fatal(err)
						}
						courtReference[scotusReference[line[0]]][header[i]] = primitive.NewDateTimeFromTime(tme)
					case 5,10,11,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,48,50,51,52:
						courtReference[scotusReference[line[0]]][header[i]], err = strconv.ParseInt(v,10,64)
						if err != nil {
						log.Fatal(err)
						}
					case 6,7,8,9,47,14,12,13:
						courtReference[scotusReference[line[0]]][header[i]] = v
					}
				}
			
			}
			currCase = newCurrCase
		}

		justice, err := strconv.ParseInt(line[53],10,64)
		courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)] = make(map[string]interface{})
		courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)]["opinion"] = "" 
		for k, v := range line[53:] {
			if err != nil {
				log.Fatal(err)
			}
			if v == "" {
				courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)][header[53+k]] = nil

			} else {
				switch k {
				case 0,2,3,4,5,6,7:
					courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)][header[53+k]], err = strconv.ParseInt(v, 10, 64)
					if err != nil {
						log.Fatal(err)
					}
				case 1:
					courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)][header[53+k]] = v 
				}	
			}
		}
	n++
	}
	return courtReference, nil

}

func getOldScotus(scotusReference map[string]string, path string, courtReference map[string]map[string]interface{}) (map[string]map[string]interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	n := 0
	currCase := ""
	lines, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	for _, line := range lines {
		header := lines[0]
		
		if _, caseExists := scotusReference[line[0]]; !(caseExists) {
		//	fmt.Println("case not found: " + line[0])
			//os.Exit(1)
			continue 
		}
		newCurrCase := line[0]

		if currCase != newCurrCase {
			courtReference[scotusReference[line[0]]] = make(map[string]interface{})
			courtReference[scotusReference[line[0]]]["votes"] = make(map[int]map[string]interface{})
			for i, v := range line[:53] {
				if v == "" {
					courtReference[scotusReference[line[0]]][header[i]] = nil
				} else {
					switch i {
					case 3:
						strarr := strings.Split(v, "-")
						var intarray[6]int64
						for is, vs := range strarr {
							intarray[is], err = strconv.ParseInt(vs, 10, 64)
							if err != nil {
								log.Fatal(err)
							}
						}
						courtReference[scotusReference[line[0]]]["voteId"] = intarray
					case 4,15,16:
						tme,err := time.Parse("1/2/2006", v)
						if err != nil {
							log.Fatal(err)
						}
						courtReference[scotusReference[line[0]]][header[i]] = primitive.NewDateTimeFromTime(tme)
					case 5,10,11,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,48,50,51,52:
						courtReference[scotusReference[line[0]]][header[i]], err = strconv.ParseInt(v,10,64)
						if err != nil {
						log.Fatal(err)
						}
					case 6,7,8,9,47,14,12,13:
						courtReference[scotusReference[line[0]]][header[i]] = v
					}
				}
			
			}
			currCase = newCurrCase
		}

		justice, err := strconv.ParseInt(line[53],10,64)
		courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)] = make(map[string]interface{})
		courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)]["opinion"] = "" 
		for k, v := range line[53:] {
			if err != nil {
				log.Fatal(err)
			}
			if v == "" {
				courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)][header[53+k]] = nil

			} else {
				switch k {
				case 0,2,3,4,5,6,7:
					courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)][header[53+k]], err = strconv.ParseInt(v, 10, 64)
					if err != nil {
						log.Fatal(err)
					}
				case 1:
					courtReference[scotusReference[line[0]]]["votes"].(map[int]map[string]interface{})[int(justice)][header[53+k]] = v 
				}	
			}
		}
	n++
	}
	return courtReference, nil

}

func getOldCircuit(path string) (map[[2]string]map[string]interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	n := 0
	courtReference := make(map[[2]string]map[string]interface{})
	lines, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	header := lines[0][1:]
	for _, line := range lines[1:] {
		dateDecision,err := time.Parse("1/2/2006", (line[4]+"/"+line[5]+"/"+line[3]))
		if err != nil {
			log.Fatal(err)
		}

		index := [2]string{line[8], dateDecision.Format("2006-01-02")}
		courtReference[index] = make(map[string]interface{})
		courtReference[index]["votes"] = make(map[int]map[string]interface{})
		courtReference[index]["dateDecision"] = dateDecision
		for i, v := range line[1:156] {
			if v == "" {
				courtReference[index][header[i]] = nil
			} else {
				switch i {
				case 5:
					strarr := strings.Split(v, "/")
					var intarray[2]int64
					for is, vs := range strarr {
						intarray[is], err = strconv.ParseInt(vs, 10, 64)
						if err != nil {
							log.Fatal(err)
						}
					}
					courtReference[index]["cite"] = intarray
				case 2,3,4:
					continue
				case 1,21:
					courtReference[index][header[i]] = v
				case 18:
					strarr := strings.Split(v, ".")
					courtReference[index][header[i]], err = strconv.ParseInt(strarr[0],10,64)
					if err != nil {
					log.Fatal(err)
					}
	
				default:
					//print(header[i])
					//println(" " + v)
					courtReference[index][header[i]], err = strconv.ParseInt(v,10,64)
					if err != nil {
					log.Fatal(err)
					}
				}
			}
		
		}

		justice, err := strconv.ParseInt(line[156],10,64)
		if err != nil {
			log.Fatal(err)
		}
		for k, v := range line[156:228] {
			if v == "" {
				continue
			}
			vi, err := strconv.ParseInt(v, 10, 64) 
			if err != nil {
				log.Fatal(err)
			}
			if strings.Contains(header[155+k], "code") {
				justice, err = strconv.ParseInt(line[156+k],10,64)
				if err != nil {
					log.Fatal(err)
				}
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)] = make(map[string]interface{})
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)]["opinion"] = "" 
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)][header[155+k][4:] + "code"] = vi
			} else {
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)][header[155+k]] = vi
			}
		}
		for j, m := range line[228:] {
			if m == "" {
				courtReference[index][header[227+j]] = nil
			} else {
				courtReference[index][header[227+j]], err = strconv.ParseInt(m,10,64)
				if err != nil {
				log.Fatal(err)
				}	
			}
		}
	n++
	}
	return courtReference, nil

}

func getNewCircuit(path string, courtReference map[[2]string]map[string]interface{}) (map[[2]string]map[string]interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	n := 0
	lines, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	header := lines[0][1:]
	for _, line := range lines[1:] {
		slc := strings.Split(line[7], "/")
		dateDecision,err := time.Parse("1/2/2006", (slc[0]+"/"+slc[1]+"/"+line[2]))
		if err != nil {
			log.Fatal(err)
		}

		index := [2]string{line[4], dateDecision.Format("2006-01-02")}
		courtReference[index] = make(map[string]interface{})
		courtReference[index]["votes"] = make(map[int]map[string]interface{})
		courtReference[index]["dateDecision"] = dateDecision
		for i, v := range line[1:180] {
			if v == "" {
				courtReference[index][header[i]] = nil
			} else {
				switch i {
				case 7:
					strarr := strings.Split(v, "-")
					courtReference[index]["cite"] = strarr
				case 6:
					continue
				case 1,25,39,40,41,65,66,67,70,26,27,42,43,68,69,107:
					courtReference[index][header[i]] = v
				case 18:
					strarr := strings.Split(v, ".")
					courtReference[index][header[i]], err = strconv.ParseInt(strarr[0],10,64)
					if err != nil {
					log.Fatal(err)
					}
	
				default:
					//print(header[i])
					//println(" " + v)
					courtReference[index][header[i]], err = strconv.ParseInt(v,10,64)
					if err != nil {
					log.Fatal(err)
					}
				}
			}
		
		}

		justice, err := strconv.ParseInt(line[156],10,64)
		if err != nil {
			log.Fatal(err)
		}
		for k, v := range line[180:] {
			if v == "" {
				continue
			}
			vi, err := strconv.ParseInt(v, 10, 64) 
			if err != nil {
				log.Fatal(err)
			}
			if strings.Contains(header[179+k], "code") {
				justice, err = strconv.ParseInt(line[180+k],10,64)
				if err != nil {
					log.Fatal(err)
				}
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)] = make(map[string]interface{})
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)]["opinion"] = "" 
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)][header[179+k][4:]+"code"] = vi
			} else {
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)][header[179+k]] = vi
			}
		}
	n++
	}
	return courtReference, nil

}

func getDistrict(path string) (map[[2]string]map[string]interface{}, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	n := 0
	courtReference := make(map[[2]string]map[string]interface{})
	lines, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	header := lines[0][1:]
	for _, line := range lines[1:] {
		casenum := strings.Split(line[12], ".")
		year := strings.Split(line[8], ".")[0]
		month := strings.Split(line[7], ".")[0]
		index := [2]string{casenum[0][len(casenum[0])-3:], (year + "-" + month)}
		courtReference[index] = make(map[string]interface{})
		courtReference[index]["opinion"] = ""
		for i, v := range line[1:] {
			if v == "" {
				courtReference[index][header[i]] = nil
			} else {
				vv, err := strconv.ParseInt((strings.Split(v, "."))[0], 10, 64)
				if err != nil {
					log.Fatal(err)
				}
				courtReference[index][header[i]] = vv
			}
		
		}
	n++
	}
	return courtReference, nil

}

func getCircuitJudge(cpath string, spath string)  (map[int64]string, map[int64]string, error) {
	file, err := os.Open(cpath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	n := 0
	circuitReference := make(map[int64]string)
	lines, err := csvReader.ReadAll()	
	if err != nil {
		log.Fatal(err)
	}
	for _, line := range lines[1:] {
		if line[28] == "" {
			continue
		}	
		id, err := strconv.ParseInt(line[28], 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		circuitReference[id] = strings.ToLower(line[19])
		n++
	}
	file, err = os.Open(spath)
	if err != nil {
		log.Fatal(err)
	}

	csvReader = csv.NewReader(file)
	n = 0
	scotusReference := make(map[int64]string)
	lines, err = csvReader.ReadAll()	
	if err != nil {
		log.Fatal(err)
	}
	for _, line := range lines[1:] {
		if strings.Contains(line[9], "spaeth") {
			continue
		}	

		id, err := strconv.ParseInt(line[9], 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		scotusReference[id] = strings.ToLower(line[1])
		n++
	}

	return circuitReference, scotusReference, nil
}


func scotusAuthorString(authors string) []string {
	authorss := strings.Split(authors, ",")
	pauthors := make([]string, len(authorss))
	for i, v := range authorss {
		eauthor := strings.Split(v, " ")
		pauthors[i] = eauthor[len(eauthor) - 1]
	}
	for i, v := range pauthors {
		pauthors[i] = strings.ToLower(v)
		if v == "" {
			pauthors[i] = pauthors[len(pauthors)-1]
			pauthors = pauthors[:len(pauthors)-1]
		}
	}
	return pauthors
}

func attachScotus(scotusJudgeReference map[int64]string, scotusReference map[string]map[string]interface{}, scotusCase map[string]interface{}) (map[int64]string, map[string]map[string]interface{}) {
		if matchedCase, exists := scotusReference[fmt.Sprintf("%.f", scotusCase["id"].(float64))]; exists {
			JudgeLoop:
				for _, values := range matchedCase["votes"].(map[int]map[string]interface{})	{
					opinions := scotusCase["casebody"].(map[string]interface{})["data"].(map[string]interface{})["opinions"].([]interface{})
					// check for a cited name
					for _, opinion := range opinions {
						fOpinion := opinion.(map[string]interface{})
						for _, m := range scotusAuthorString(fOpinion["author"].(string)) {
							if strings.Contains(scotusJudgeReference[values["justice"].(int64)], m) {
								values["opinion"] = fOpinion["text"].(string)
								err := errors.New("")
								matchedCase["harvardID"], err = strconv.ParseInt(fmt.Sprintf("%.f", scotusCase["id"].(float64)), 10, 64)
								if err != nil {
									log.Fatal(err)
								}
								continue JudgeLoop
							}
						} 
					}
					// check for an agreement
					if values["firstAgreement"] != nil {
						for _, opinion := range opinions {
							fOpinion := opinion.(map[string]interface{})
							for _, m := range scotusAuthorString(fOpinion["author"].(string)) {
								if strings.Contains(scotusJudgeReference[values["firstAgreement"].(int64)], m) {
									values["opinion"] = fOpinion["text"].(string)
									err := errors.New("")
									matchedCase["harvardID"], err = strconv.ParseInt(fmt.Sprintf("%.f", scotusCase["id"].(float64)), 10, 64)
									if err != nil {
										log.Fatal(err)
									}
									continue JudgeLoop
								}
							} 
						}
					}
					// default to majority opinion
					if ((values["vote"].(int64) == int64(1))) {
						for _, opinion := range opinions {
							fOpinion := opinion.(map[string]interface{})
							if fOpinion["type"].(string) == "majority" {
								values["opinion"] = fOpinion["text"].(string)
								err := errors.New("")
								matchedCase["harvardID"], err = strconv.ParseInt(fmt.Sprintf("%.f", scotusCase["id"].(float64)), 10, 64)
								if err != nil {
									log.Fatal(err)
								}
								continue JudgeLoop
							}
						} 
					} 
					fmt.Println("No opinion found, judge: " + values["justice"].(string) + " case: " + matchedCase["usCite"].(string))
				} 
		}
		return scotusJudgeReference, scotusReference
}

func circuitAuthorString(authors string) [][]string {
	authorss := strings.Split(authors, ",")
	pauthors := make([]string, len(authorss))
	for i, v := range authorss {
		pauthors[i] = strings.ToLower(v)
		if ((v == "") || (strings.Contains(v, "Judge"))) {
			pauthors[i] = pauthors[len(pauthors)-1]
			pauthors = pauthors[:len(pauthors)-1]
		}
	}
	fauthors := make([][]string, len(pauthors))
	for i, v := range pauthors {
		fauthors[i] = make([]string, len(strings.Fields(v)))
		fauthors[i] = strings.Fields(v)
	}
	return fauthors
}
func findVoteCode(votes map[string]interface{}) string {
	for i := range votes {
		if strings.Contains(i, "maj") {
			return i
		}
	} 
	return ""
}

func attachCircuit(circuitJudgeReference map[int64]string, circuitReference map[[2]string]map[string]interface{}, circuitCase map[string]interface{}) (map[int64]string, map[[2]string]map[string]interface{}) {
	if matchedCase, exists := circuitReference[[2]string{circuitCase["first_page"].(string), circuitCase["decision_date"].(string)}]; exists {
		JudgeLoop:
			for judge, values := range matchedCase["votes"].(map[int]map[string]interface{})	{
				// check for a cited name
				opinions := circuitCase["casebody"].(map[string]interface{})["data"].(map[string]interface{})["opinions"].([]interface{})
				for _, opinion := range opinions {
					fOpinion := opinion.(map[string]interface{})
					JudgeUpNameLoop:	
					for _, m := range circuitAuthorString(fOpinion["author"].(string)) {
						curJudgeName := circuitJudgeReference[int64(judge)]
						JudgeNameLoop:
						for i, g := range m {
							if strings.Contains(curJudgeName, g) && i == (len(m) - 1) {
								values["opinion"] = fOpinion["text"].(string)
								err := errors.New("")
								matchedCase["harvardID"], err = strconv.ParseInt(fmt.Sprintf("%.f", circuitCase["id"].(float64)), 10, 64)
								if err != nil {
									log.Fatal(err)
								}
								matchedCase["cite"] = circuitCase["citations"].([]interface{})[0].(map[string]interface{})["cite"].(string)
								matchedCase["name"] = circuitCase["name"].(string)
								matchedCase["name_abv"] = circuitCase["name_abbreviation"].(string)
								continue JudgeLoop
							} else if strings.Contains(curJudgeName, g) {
								continue JudgeNameLoop
							} else if !(strings.Contains(curJudgeName, g)) {
								continue JudgeUpNameLoop
							}
						}	
					} 
				}
				voteCode := findVoteCode(values)
				// if majority, grab majority opinion
				if values[voteCode].(int64) == int64(1) {
					for _, opinion := range opinions {
						fOpinion := opinion.(map[string]interface{})
						if fOpinion["type"].(string) == "majority" {
							values["opinion"] = fOpinion["text"].(string)
							err := errors.New("")
							matchedCase["harvardID"], err = strconv.ParseInt(fmt.Sprintf("%.f", circuitCase["id"].(float64)), 10, 64)
							if err != nil {
								log.Fatal(err)
							}
							matchedCase["cite"] = circuitCase["citations"].([]interface{})[0].(map[string]interface{})["cite"].(string)
							matchedCase["name"] = circuitCase["name"].(string)
							matchedCase["name_abv"] = circuitCase["name_abbreviation"].(string)
							continue JudgeLoop
						}
					} 
				}
				// if dissent, grab dissent opinion
				if ((values[voteCode].(int64) == int64(2))) {
					for _, opinion := range opinions {
						fOpinion := opinion.(map[string]interface{})
						if fOpinion["type"].(string) == "dissent" {
							values["opinion"] = fOpinion["text"].(string)
							err := errors.New("")
							matchedCase["harvardID"], err = strconv.ParseInt(fmt.Sprintf("%.f", circuitCase["id"].(float64)), 10, 64)
							if err != nil {
								log.Fatal(err)
							}
							matchedCase["cite"] = circuitCase["citations"].([]interface{})[0].(map[string]interface{})["cite"].(string)
							matchedCase["name"] = circuitCase["name"].(string)
							matchedCase["name_abv"] = circuitCase["name_abbreviation"].(string)
							continue JudgeLoop
						}
					} 
				}
				fmt.Println("No opinion found, judge: " + circuitJudgeReference[int64(judge)] + " case: " +  circuitCase["citations"].([]interface{})[0].(map[string]interface{})["cite"].(string))
			} 
		}
	return circuitJudgeReference, circuitReference
}

func attachDistrict(districtReference map[[2]string]map[string]interface{}, districtCase map[string]interface{}) (map[[2]string]map[string]interface{}) {
	datePF, err := time.Parse("2006-01-02", districtCase["decision_date"].(string))
	if err != nil {
		log.Fatal(err)
	}
	dateF := datePF.Format("2006-1")
	if matchedCase, exists := districtReference[[2]string{districtCase["first_page"].(string), dateF}]; exists {
		matchedCase["opinion"] = districtCase["casebody"].(map[string]interface{})["data"].(map[string]interface{})["opinions"].([]interface{})[0].(map[string]interface{})["text"].(string)
		err := errors.New("")
		matchedCase["harvardID"], err = strconv.ParseInt(fmt.Sprintf("%.f", districtCase["id"].(float64)), 10, 64)
		if err != nil {
			log.Fatal(err)
		}
		matchedCase["cite"] = districtCase["citations"].([]interface{})[0].(map[string]interface{})["cite"].(string)
		matchedCase["name"] = districtCase["name"].(string)
		matchedCase["name_abv"] = districtCase["name_abbreviation"].(string)

	}
	return districtReference
}

func attachOpinions(path string, scotusJudgeReference map[int64]string, scotusReference map[string]map[string]interface{}, circuitJudgeReference map[int64]string, circuitReference map[[2]string]map[string]interface{}, districtReference map[[2]string]map[string]interface{}) (map[string]map[string]interface{}, map[[2]string]map[string]interface{}, map[[2]string]map[string]interface{}) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, (256 * 1024))
	scanner.Buffer(buf, (10*1024*1024))

	for scanner.Scan() {
		fCase := make(map[string]interface{})
		err := json.Unmarshal(scanner.Bytes(), &fCase)
		if err != nil {
			log.Fatal(err)
		}
		court := fCase["court"].(map[string]interface{})["name"].(string)
		if strings.Contains(court, "Supreme") {
			scotusJudgeReference, scotusReference = attachScotus(scotusJudgeReference, scotusReference, fCase) 
			continue
		} else if strings.Contains(court, "Appeals") {
			circuitJudgeReference, circuitReference = attachCircuit(circuitJudgeReference, circuitReference, fCase)
			continue
		} else if strings.Contains(court, "District") {
			districtReference = attachDistrict(districtReference, fCase)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return scotusReference, circuitReference, districtReference
}

func sendScotus(scotusCaseReference map[string]map[string]interface{}, client *mongo.Client) {
	return
	db := client.Database("scotus")	
	for _, c := range scotusCaseReference {
		for _, o := range c["votes"].(map[int]map[string]interface{}) {
			vote := c
			for k, b := range o {
				vote[k] = b
			}
			delete(vote, "votes")	
			coll := db.Collection(strconv.Itoa(int(vote["justice"].(int64))))
			_, err := coll.InsertOne(context.TODO(), vote)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func sendCircuit(circuitCaseReference map[[2]string]map[string]interface{}, client *mongo.Client) {
	db := client.Database("circuit")	
	for _, c := range circuitCaseReference {
		for _, o := range c["votes"].(map[int]map[string]interface{}) {
			vote := c
			for k, b := range o {
				if k != "opinion" {
					vote[k[2:]] = b
				}
				vote[k]	= b
			}
			delete(vote, "votes")
			coll := db.Collection(strconv.Itoa(int(vote["code"].(int64))))
			_, err := coll.InsertOne(context.TODO(), vote)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
}

func sendDistrict(districtCaseReference map[[2]string]map[string]interface{}, client *mongo.Client) {
	db := client.Database("admin")	
	for _, c := range districtCaseReference {
		coll := db.Collection(strconv.Itoa(int(c["judge"].(int64))))
		_, err := coll.InsertOne(context.TODO(), c)
		if err != nil {
			log.Fatal(err)
		}
	}
}