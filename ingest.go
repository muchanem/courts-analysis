/*
ingests the ~29 gigabytes of courts data contained to a mongodb database
*/
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"log"
	//"time"
	//"context"
	"encoding/csv"
	"io"
	"strconv"
	"errors"
	//"go.mongodb.org/mongo-driver/mongo"
	//"go.mongodb.org/mongo-driver/mongo/options"
	//"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strings"
	"time"
	"bufio"

)

func main() {
/*	
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
*/ /*
	scotusReference, err := readScotusReference("./data/scdb_matchup_2020-01-16.csv")
	if err != nil {
		log.Println(err)
	}
	scotusCaseReference, err := getNewScotus(scotusReference, "./data/SCDB_2020_01_justiceCentered_Citation.csv")
	if err != nil {
		log.Println(err)
	}
	fmt.Println(len(scotusCaseReference))
	scotusCaseReference, err = getOldScotus(scotusReference, "./data/SCDB_Legacy_06_justiceCentered_Citation.csv", scotusCaseReference) 
	if err != nil {
		log.Println(err)
	}

	fmt.Println(len(scotusCaseReference))
	fmt.Println(scotusCaseReference["12407356"])
*/ /*
	circuitCaseReference, err := getOldCircuit("./data/cta96.csv")
	if err != nil {
		log.Println(err)
	}

	circuitCaseReference, err = getNewCircuit("./data/cta02.csv", circuitCaseReference)
	if err != nil {
		log.Println(err)
	} */
	
	circuitJudgeReference, scotusJudgeReference, err := getCircuitJudge("./data/appct_judges.csv", "./data/justicesdata2021.csv")
	if err != nil {
		log.Println(err)
	}
    /*
	districtCaseReference, err := getDistrict("./data/fdcdata.csv")
	if err != nil {
		log.Println(err)
	} */

	scotusCaseReference, circuitCaseReference, districtCaseReference  = attachOpinions("./data", scotusJudgeReference, scotusCaseReference, circuitCaseReference, circuitJudgeReference, districtCaseReference)
	if err != nil {
		log.Println(err)
	}
	scotusCases, circuitCases, districtCases := make([]map[string]interface{}, 0), make([]map[string]interface{}, 0), make([]map[string]interface{}, 0)
	for s, c := range scotusCaseReference {
		for v, o := range c["votes"].(map[int64]map[string]interface{}) {
			scotusCases = append(scotusCases, append(scotusCaseReference[s], o...))
		}
	}	

	for i, r := range circuitCaseReference {
		for v, o := range r["votes"].(map[int64]map[string]interface{}) {
			for k, i := range o {
				nKey := k[2:]
				o[nKey] = o[k]
				delete(o, k)
			}
			circuitCases = append(circuitCases, append(circuitCaseReference[i], o...))
		}
	}	

	for d, t := range districtCaseReference {
		districtCases = append(districtCases, t)
	}

	sendScotus(scotusCases, client)
	sendCircuit(circuitCases, client)
	sendDistrict(districtCases, client)
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
			if strings.Contains(header[155+k], "code") {
				justice, err = strconv.ParseInt(line[156+k],10,64)
				if err != nil {
					log.Fatal(err)
				}
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)] = make(map[string]interface{})
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)]["opinion"] = "" 
			}
			courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)][header[155+k]] = v
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
			if strings.Contains(header[179+k], "code") {
				justice, err = strconv.ParseInt(line[180+k],10,64)
				if err != nil {
					log.Fatal(err)
				}
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)] = make(map[string]interface{})
				courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)]["opinion"] = "" 
			}
			courtReference[index]["votes"].(map[int]map[string]interface{})[int(justice)][header[179+k]] = v
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
		
		id, err := strconv.ParseInt(line[2], 10, 64)
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
		if matchedCase, exists := scotusReference[scotusCase["id"].(string)]; exists {
			JudgeLoop:
				for _, values := range matchedCase["votes"].(map[int64]map[string]interface{})	{
					// check for a cited name
					for _, opinion := range scotusCase["opinions"].([]map[string]string) {
						for _, m := range scotusAuthorString(opinion["author"]) {
							if strings.Contains(scotusJudgeReference[values["justice"].(int64)], m) {
								values["opinion"] = opinion["text"]
								matchedCase["harvardID"] = scotusCase["id"].(int64)
								continue JudgeLoop
							}
						} 
					}
					// check for an agreement
					if values["firstAgreement"] != nil {
						for _, opinion := range scotusCase["opinions"].([]map[string]string) {
							for _, m := range scotusAuthorString(opinion["author"]) {
								if strings.Contains(scotusJudgeReference[values["firstAgreement"].(int64)], m) {
									values["opinion"] = opinion["text"]
									matchedCase["harvardID"] = scotusCase["id"].(int64)
									continue JudgeLoop
								}
							} 
						}
					}
					// default to majority opinion
					if ((values["vote"].(int64) == int64(1))) {
						for _, opinion := range scotusCase["opinions"].([]map[string]string) {
							if opinion["type"] == "majority" {
								values["opinion"] = opinion["text"]
								matchedCase["harvardID"] = scotusCase["id"].(int64)
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
			for judge, values := range matchedCase["votes"].(map[int64]map[string]interface{})	{
				// check for a cited name
				for _, opinion := range circuitCase["opinions"].([]map[string]string) {
					JudgeUpNameLoop:	
					for _, m := range circuitAuthorString(opinion["author"]) {
						curJudgeName := circuitJudgeReference[judge]
						JudgeNameLoop:
						for i, g := range m {
							if strings.Contains(curJudgeName, g) && i == (len(g) - 1) {
								values["opinion"] = opinion["text"]
								matchedCase["harvardID"] = circuitCase["id"].(int64)
								matchedCase["cite"], matchedCase["name"], matchedCase["name_abv"] = circuitCase["citations"].([]map[string]string)[0]["cite"], circuitCase["name"].(string), circuitCase["name_abbreviation"].(string)
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
					for _, opinion := range circuitCase["opinions"].([]map[string]string) {
						if opinion["type"] == "majority" {
							values["opinion"] = opinion["text"]
							matchedCase["harvardID"] = circuitCase["id"].(int64)
							matchedCase["cite"], matchedCase["name"], matchedCase["name_abv"] = circuitCase["citations"].([]map[string]string)[0]["cite"], circuitCase["name"].(string), circuitCase["name_abbreviation"].(string)
							continue JudgeLoop
						}
					} 
				}
				// if dissent, grab dissent opinion
				if ((values[voteCode].(int64) == int64(2))) {
					for _, opinion := range circuitCase["opinions"].([]map[string]string) {
						if opinion["type"] == "dissent" {
							values["opinion"] = opinion["text"]
							matchedCase["harvardID"] = circuitCase["id"].(int64)
							matchedCase["cite"], matchedCase["name"], matchedCase["name_abv"] = circuitCase["citations"].([]map[string]string)[0]["cite"], circuitCase["name"].(string), circuitCase["name_abbreviation"].(string)
							continue JudgeLoop
						}
					} 
				}
				fmt.Println("No opinion found, judge: " + circuitJudgeReference[judge] + " case: " + circuitCase["citations"].([]map[string]string)[0]["cite"])
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
	if matchedCase, exists := districtReference[[2]string{districtCase["last_page"].(string), dateF}]; exists {
		matchedCase["opinion"] = districtCase["opinions"].([]map[string]string)[0]["text"]
		matchedCase["harvardID"] = districtCase["id"].(int64)
		matchedCase["cite"], matchedCase["name"], matchedCase["name_abv"] = districtCase["citations"].([]map[string]string)[0]["cite"], districtCase["name"].(string), districtCase["name_abbreviation"].(string)
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
	buf := make([]byte, (256 * 1024))
	scanner.Buffer(buf, (256*1024))

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

func sendScotus(scotusCases []map[string]interface{}, client *mongo.Client) {
	db := client.Database("scotus")	
	for _, v := range scotusCases {
		coll := db.Collection(v["justice"])
		_, err := coll.InsertOne(context.TODO(), v)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func sendCircuit(circuitCases []map[string]interface{}, client *mongo.Client) {
	db := client.Database("circuit")	
	for _, v := range circuitCases {
		coll := db.Collection(v["code"])
		_, err := coll.InsertOne(context.TODO(), v)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func sendDistrict(districtCases []map[string]interface{}, client *mongo.Client) {
	db := client.Database("district")	
	for _, v := range districtCases {
		coll := db.Collection(v["judge"])
		_, err := coll.InsertOne(context.TODO(), v)
		if err != nil {
			log.Fatal(err)
		}
	}
}