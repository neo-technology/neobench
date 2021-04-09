package builtin

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/pkg/errors"
	"math/rand"
	"neobench/pkg/neobench"
	"time"
)

const LDBCIC2 = `
\set personId random(1, 9892 * $scale)

MATCH (:Person {id:{personId}})-[:KNOWS]-(friend),
      (friend)<-[:HAS_CREATOR]-(message)
WHERE message.creationDate <= date({year: 2010, month:10, day:10})
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       message.id AS messageId,
       coalesce(message.content, message.imageFile) AS messageContent,
       message.creationDate AS messageDate
ORDER BY messageDate DESC, messageId ASC
LIMIT 20
`

const LDBCIC6 = `
\set personId random(1, 9892 * $scale)
\set tagId random(1, 16080)

MATCH (knownTag:Tag {name: "Tag-" + $tagId})
MATCH (person:Person {id:$personId})-[:KNOWS*1..2]-(friend)
WHERE NOT person=friend
WITH DISTINCT friend, knownTag
MATCH (friend)<-[:HAS_CREATOR]-(post)
WHERE (post)-[:HAS_TAG]->(knownTag)
WITH post, knownTag
MATCH (post)-[:HAS_TAG]->(commonTag)
WHERE NOT commonTag=knownTag
WITH commonTag, count(post) AS postCount
RETURN commonTag.name AS tagName, postCount
ORDER BY postCount DESC, tagName ASC
LIMIT 10;
`

const LDBCIC10 = `
\set personId random(1, 9892 * $scale)
\set birthdayMonth random(1, 13)

MATCH (person:Person {id:$personId})-[:KNOWS*2..2]-(friend),
       (friend)-[:IS_LOCATED_IN]->(city)
WHERE NOT friend=person AND
      NOT (friend)-[:KNOWS]-(person) AND
            ( (friend.birthday.month=$birthdayMonth AND friend.birthday.day>=21) OR
        (friend.birthday.month=($birthdayMonth%12)+1 AND friend.birthday.day<22) )
WITH DISTINCT friend, city, person
OPTIONAL MATCH (friend)<-[:HAS_CREATOR]-(post)
WITH friend, city, collect(post) AS posts, person
WITH friend,
     city,
     size(posts) AS postCount,
     size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]) AS commonPostCount
RETURN friend.id AS personId,
       friend.firstName AS personFirstName,
       friend.lastName AS personLastName,
       friend.gender AS personGender,
       city.name AS personCityName,
       commonPostCount - (postCount - commonPostCount) AS commonInterestScore
ORDER BY commonInterestScore DESC, personId ASC
LIMIT 10;
`

const LDBCIC14 = `
\set personOne random(1, 9892 * $scale)
\set personTwo random(1, 9892 * $scale)

MATCH path = allShortestPaths((person1:Person {id:$personOne})-[:KNOWS*0..]-(person2:Person {id:$personTwo}))
RETURN
 [n IN nodes(path) | n.id] AS pathNodeIds,
 reduce(weight=0.0, r IN relationships(path) |
            weight +
            size(()-[r]->()<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->()-[r]->())*1.0 +
            size(()<-[r]-()<-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]->(:Post)-[:HAS_CREATOR]->()<-[r]-())*1.0 +
            size(()<-[r]-()-[:HAS_CREATOR]-(:Comment)-[:REPLY_OF]-(:Comment)-[:HAS_CREATOR]-()<-[r]-())*0.5
 ) AS weight
ORDER BY weight DESC;
`

const ldbcStartYear = 2002

// This populates a dataset that follows the LDBC SNB schema and attempts to achieve superficially similar
// distributions. It is *not* LDBC, but it is intended as a proxy for it. Ideally, if you have a setup that
// works well with this benchmark, it'd also do well in the real LDBC benchmark.
//
// The primary thing you get here is a dataset that can have load generated against it without coordination;
// names and identities are deterministically generated for a given seed and scale.
//
// The generation works by first populating the static portion - places, tags etc - and then simulating
// ten years worth of activity in the social network, with users joining over time, creating new forums,
// forming new friendships and so on.
func InitLDBCLike(scale, seed int64, dbName string, driver neo4j.Driver, out neobench.Output) error {
	random := rand.New(rand.NewSource(seed))

	numContinents := int64(6)
	numCountries := int64(111)
	numCities := int64(1343)

	numUniversities := int64(6380)
	numCompanies := int64(1575)

	numTags := int64(16080)
	numTagClasses := int64(71)

	numPeople := 9892 * scale

	now := time.Date(ldbcStartYear, 1, 1, 0, 0, 0, 0, time.UTC)
	daysOfActivity := 365 * 10

	session := driver.NewSession(neo4j.SessionConfig{
		AccessMode:   neo4j.AccessModeWrite,
		DatabaseName: dbName,
	})
	defer session.Close()

	// Make sure we're working against a db with no ldbc data in it; we are not (yet!) reentrant
	result, err := session.Run("MATCH (p:Person) RETURN COUNT(p) AS numPeople", nil)
	if err != nil {
		return err
	}
	result.Next()
	preExistingPeople := int(result.Record().GetByIndex(0).(int64))
	if preExistingPeople > 0 {
		return fmt.Errorf("there appears to be data in the target database already; please note that the ldbc dataset generator is not yet re-entrant, it only works against empty graphs")
	}

	// Schema
	err = ensureSchema(session, []schemaEntry{
		{Label: "Continent", Property: "name", Unique: true},
		{Label: "City", Property: "name", Unique: true},
		{Label: "Country", Property: "name", Unique: true},
		{Label: "Country", Property: "id", Unique: true},

		{Label: "Person", Property: "id", Unique: true},
		{Label: "TagClass", Property: "name", Unique: true},
		{Label: "Tag", Property: "id", Unique: true},
		{Label: "Tag", Property: "name", Unique: true},
		{Label: "Forum", Property: "id", Unique: true},
		{Label: "Message", Property: "id", Unique: true},

		{Label: "Person", Property: "birthday_day", Unique: false},
		{Label: "Person", Property: "birthday_month", Unique: false},
		{Label: "Person", Property: "firstName", Unique: false},
		{Label: "Person", Property: "lastName", Unique: false},
		{Label: "Message", Property: "creationDate", Unique: false},
	})
	if err != nil {
		return errors.Wrapf(err, "failed to do schema setup")
	}

	out.ReportProgress(neobench.ProgressReport{
		Section:      "init",
		Step:         "create static graph portion",
		Completeness: 0,
	})

	// Places
	err = runQ(session, `UNWIND $places AS place
WITH place[0] as continentName, place[1] as countryName, place[2] as cityName
MERGE (continent:Continent {name: continentName, uri: "https://continents.com/" + continentName})
MERGE (country:Country {name: countryName, uri: "https://countries.com/" + countryName})
MERGE (country)-[:IS_PART_OF]-(continent)
MERGE (city:City {name: cityName, uri: "https://cities.com/" + cityName})
MERGE (city)-[:IS_PART_OF]->(country)
`, map[string]interface{}{
		"places": generateLDBCPlaces(random, numContinents, numCountries, numCities),
	})
	if err != nil {
		return err
	}

	// Organizations
	err = runQ(session, `UNWIND $universities AS row
WITH row[0] as cityName, row[1] as uniName
MATCH (city:City {name: cityName})
MERGE (uni:University {name: uniName, url: "https://university.edu/" + uniName})
MERGE (uni)-[:IS_LOCATED_IN]->(city)
`, map[string]interface{}{
		"universities": generateLDBCUniversities(random, numCities, numUniversities),
	})
	if err != nil {
		return err
	}

	err = runQ(session, `UNWIND $companies AS row
WITH row[0] as countryName, row[1] as corpName
MATCH (country:Country {name: countryName})
MERGE (corp:Country {name: corpName, url: "https://corp.com/" + corpName})
MERGE (corp)-[:IS_LOCATED_IN]->(country)
`, map[string]interface{}{
		"companies": generateLDBCCompanies(random, numCities, numCompanies),
	})
	if err != nil {
		return err
	}

	// TagClasses
	err = runQ(session, `MERGE (root:TagClass {name: "TagClass-0"}) ON CREATE SET root.url = "https://tagclass.com/tagclass-0"
WITH root
UNWIND $classes as row
WITH row[0] as className, row[1] as parentName
MERGE (c:TagClass {name: className, url: "https://tagclass.com/" + className})
WITH c, parentName
MATCH (p:TagClass {name: parentName})
MERGE (c)-[:IS_SUBCLASS_OF]->(p)
`, map[string]interface{}{
		"classes": generateLDBCTagClasses(random, numTagClasses),
	})
	if err != nil {
		return err
	}

	// Tags
	err = runQ(session, `
UNWIND $tags as row
WITH row[0] as tagName, row[1] as className
MERGE (c:Tag {name: tagName, url: "https://tag.com/" + tagName})
WITH c, className
MATCH (p:TagClass {name: className})
MERGE (c)-[:HAS_TYPE]->(p)
`, map[string]interface{}{
		"tags": generateLDBCTags(random, numTags, numTagClasses),
	})
	if err != nil {
		return err
	}

	// Dynamic graph portion

	out.ReportProgress(neobench.ProgressReport{
		Section:      "init",
		Step:         "simulating dynamic content creation",
		Completeness: 0,
	})

	// We populate the dynamic data by simulating user activity; this is meant to try to
	// bring the initial dataset to the same state it'd be in in a real world social network

	signupsPerDay := float64(numPeople) / float64(daysOfActivity)

	// These structures help shape the simulated activity such that activity skews to be between friends
	friends := &choiceMatrix32{
		entries: make([][]int32, numPeople),
		random:  random,
	}
	memberships := &choiceMatrix32{
		entries: make([][]int32, numPeople),
		random:  random,
	}

	// Helps us pick recent posts to act on
	messageCountsPerForum := make([]int, 1, 32*1024)

	signupCumulator := 0.0
	peopleCreated := 0
	forumsCreated := 0
	messagesCreated := 0
	actionsTaken := 0

	// Message ids encode their forum and an incrementing sequence; this lets us pick recent messages in a
	// given forum without coordinating with the state of the database
	newMessageId := func(forumId int) int64 {
		for len(messageCountsPerForum) <= forumId {
			messageCountsPerForum = append(messageCountsPerForum, 0)
		}
		nextMessageIndex := messageCountsPerForum[forumId]
		messageCountsPerForum[forumId] += 1
		messagesCreated += 1

		msgId := ldbcMessageId{
			forumId:      forumId,
			messageIndex: nextMessageIndex,
		}.serialize()
		return msgId
	}

	actionCreatePost := func(actor int, now time.Time) statement {
		forumId := memberships.pickExponential(actor)
		messageId := newMessageId(forumId)
		content := randLDBCMessageContent(random)
		return statement{
			query: `MATCH (p:Person {id: $personId}), (f:Forum {id: $forumId})
CREATE (m:Message:Post {
  id: $messageId,
  creationDate: $now,
  browserUsed: $browserUsed,
  locationIP: $locationIP,
  content: $content,
  length: $length,
  language: $language,
  imageFile: $imageFile
})
CREATE (f)-[:CONTAINER_OF]->(m)
CREATE (m)-[:HAS_CREATOR]->(p)
WITH m
UNWIND $tags as tag 
MATCH (t:Tag {name:tag})
CREATE (m)-[:HAS_TAG]->(t)
`,
			params: map[string]interface{}{
				"personId":    actor,
				"forumId":     forumId,
				"messageId":   messageId,
				"now":         now,
				"browserUsed": randBrowser(random),
				"locationIP":  "127.0.0.1",
				"content":     content,
				"length":      len(content),
				"language":    "uz",
				"imageFile":   "photo1374389534791.jpg",
				"tags":        randLDBCTags(random, numTags),
			},
		}
	}

	actionComment := func(actor int, now time.Time) statement {
		forumId := memberships.pickExponential(actor)
		lastMessage := messageCountsPerForum[forumId]
		if lastMessage < 1 {
			return ldbcNoop
		}
		parentIndex, _ := neobench.ExponentialRand(random, 1, int64(lastMessage), 10.0)
		parentId := ldbcMessageId{
			forumId:      forumId,
			messageIndex: int(parentIndex),
		}.serialize()
		messageId := newMessageId(forumId)

		content := randLDBCMessageContent(random)
		return statement{
			query: `MATCH (p:Person {id: $personId}), (parent:Message {id: $parentId})
CREATE (c:Message:Comment {
  id: $messageId,
  creationDate: $now,
  browserUsed: $browserUsed,
  locationIP: $locationIP,
  content: $content,
  length: $length
})
CREATE (c)-[:REPLY_OF]->(parent)
CREATE (c)-[:HAS_CREATOR]->(p)
WITH c
UNWIND $tags as tag 
MATCH (t:Tag {name:tag})
CREATE (c)-[:HAS_TAG]->(t)
`,
			params: map[string]interface{}{
				"personId":    actor,
				"parentId":    parentId,
				"messageId":   messageId,
				"now":         now,
				"browserUsed": randBrowser(random),
				"locationIP":  "127.0.0.1",
				"content":     content,
				"length":      len(content),
				"tags":        randLDBCTags(random, numTags),
			},
		}
	}

	actionLike := func(actor int, now time.Time) statement {
		forumId := memberships.pickExponential(actor)
		lastMessage := messageCountsPerForum[forumId]
		if lastMessage < 1 {
			return ldbcNoop
		}
		messageIndex, _ := neobench.ExponentialRand(random, 1, int64(lastMessage), 10.0)
		messageId := ldbcMessageId{
			forumId:      forumId,
			messageIndex: int(messageIndex),
		}.serialize()

		return statement{
			query: `MATCH (p:Person {id: $personId}), (msg:Message {id: $messageId})
CREATE (p)-[:LIKES {creationDate: $now}]->(msg)
`,
			params: map[string]interface{}{
				"personId":  actor,
				"messageId": messageId,
				"now":       now,
			},
		}
	}

	actionAddFriend := func(actor int, now time.Time) statement {
		friendId := 0
		// TODO: Weight this to favor friend-of-friends
		tries := 0
		for {
			friendId = random.Intn(peopleCreated) + 1
			if !friends.contains(actor, friendId) {
				break
			}
			tries += 1
			if tries > 10 {
				// In small graphs, we sometimes can't find any new friendships to add, make these no-ops
				return ldbcNoop
			}
		}
		friends.insert(actor, friendId)
		friends.insert(friendId, actor)
		return statement{
			query: `MATCH (p:Person {id: $personId}), (f:Person {id: $friendId})
MERGE (m)<-[:KNOWS {creationDate: $now}]-(f)
`,
			params: map[string]interface{}{
				"personId": actor,
				"friendId": friendId,
				"now":      now,
			},
		}
	}

	actionJoinForum := func(actor int, now time.Time) statement {
		if forumsCreated < 1 {
			return ldbcNoop
		}
		forumId := 0
		// TODO: Weight this to favor friends' forums
		tries := 0
		for {
			forumId = random.Intn(forumsCreated) + 1
			if !memberships.contains(actor, forumId) {
				break
			}
			tries += 1
			if tries > 10 {
				// In small graphs, we sometimes can't find any new friendships to add, make these no-ops
				return ldbcNoop
			}
		}
		memberships.insert(actor, forumId)
		return statement{
			query: `MATCH (p:Person {id: $personId}), (f:Forum {id: $forumId})
MERGE (p)<-[:HAS_MEMBER {joinDate: $now}]-(f)
`,
			params: map[string]interface{}{
				"personId": actor,
				"forumId":  forumId,
				"now":      now,
			},
		}
	}

	actionCreateForum := func(actor int, now time.Time) statement {
		forumId := forumsCreated + 1
		forumsCreated += 1

		messageCountsPerForum = append(messageCountsPerForum, 0)

		memberships.insert(actor, forumId)
		memberships.insert(actor, forumId)
		return statement{
			query: `MATCH (p:Person {id: $personId})
MERGE (f:Forum {id: $forumId})
ON CREATE SET f.title = $title, f.creationDate = $now
MERGE (f)-[:HAS_MODERATOR]->(p)
MERGE (f)-[:HAS_MEMBER {joinDate: $now}]->(p)
WITH f
UNWIND $tags as tag 
MATCH (t:Tag {name:tag})
MERGE (f)-[:HAS_TAG]->(t)
`,
			params: map[string]interface{}{
				"personId": actor,
				"forumId":  forumId,
				"now":      now,
				"title":    fmt.Sprintf("Forum %d created by Person-%d", forumId, actor),
				"tags":     randLDBCTags(random, numTags),
			},
		}
	}

	actionSetWhenNoFriends := &neobench.WeightedRandom{}
	actionSetWhenNoFriends.Add(actionAddFriend, 1)

	actionSetWhenNoMembership := &neobench.WeightedRandom{}
	actionSetWhenNoMembership.Add(actionJoinForum, 1)

	actionSetDefault := &neobench.WeightedRandom{}
	// Ratios derived from looking at dataset generated by regular ldbc datagen
	actionSetDefault.Add(actionCreateForum, 1) // Total should be ~    90,492 @ SF001
	actionSetDefault.Add(actionAddFriend, 2)   // Total should be ~   180,623 @ SF001
	actionSetDefault.Add(actionCreatePost, 11) // Total should be ~ 1,003,605 @ SF001
	actionSetDefault.Add(actionJoinForum, 16)
	actionSetDefault.Add(actionComment, 23)
	actionSetDefault.Add(actionLike, 24)

	// Each day, each user signed up so far may perform actions, but users are added linearly over time
	// 0.4 is derived from looking at LDBC-generated datasets; setting this to 0.4 gives the right
	// distribution at scale=1/SF001. I'm dubious it's correct beyond that, need to look up what
	// scale factor *means* in LDBC-land. At the same.. maybe they don't need to line up :shrug:
	// If someone wants real LDBC results, they should run LDBC; the point of this is to be *similar*
	// and to try to excercise similar choke points.
	actionsPerDayPerPerson := 0.4
	estTotalActions := int64(daysOfActivity)*int64(float64(numPeople)*actionsPerDayPerPerson/2) + numPeople
	actions := make([]statement, 0, 1024)

	performActions := func() error {
		_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			for _, action := range actions {
				res, err := tx.Run(action.query, action.params)
				if err != nil {
					return nil, errors.Wrapf(err, "for action: %s, params=%v", action.query, action.params)
				}
				_, err = res.Consume() // Need to call this to avoid bug in driver
				if err != nil {
					return nil, errors.Wrapf(err, "for action: %s, params=%v", action.query, action.params)
				}
			}
			return nil, nil
		})
		if err != nil {
			return err
		}
		return nil
	}

	for dayNo := 0; dayNo < daysOfActivity; dayNo++ {
		now = now.AddDate(0, 0, 1)
		fmt.Printf("%s (day %d, %d people, %d actions taken)\n", now, dayNo, peopleCreated, actionsTaken)
		signupCumulator += signupsPerDay
		for signupCumulator > 1 {
			signupCumulator -= 1
			actions = append(actions, createLDBCPerson(random, session, peopleCreated+1, now, numCities, numUniversities, numCompanies, numTags))
			peopleCreated += 1
		}

		if peopleCreated < 2 {
			continue
		}

		if forumsCreated < 5 {
			actions = append(actions, actionCreateForum(1, now))
		}

		actionsToday := max(1, int64(float64(peopleCreated)*actionsPerDayPerPerson))
		for actionNo := int64(0); actionNo < actionsToday; actionNo++ {
			actor := randLDBCPersonId(random, int64(peopleCreated))
			actionSet := actionSetDefault
			if friends.count(actor) == 0 {
				actionSet = actionSetWhenNoFriends
			} else if memberships.count(actor) == 0 {
				actionSet = actionSetWhenNoMembership
			}
			action := actionSet.Draw(random).(func(int, time.Time) statement)(actor, now)
			if action.query == ldbcNoop.query {
				continue
			}
			actions = append(actions, action)
			actionsTaken += 1
			if len(actions) > 1000 {
				if err := performActions(); err != nil {
					return err
				}
				actions = actions[:0]
			}
			out.ReportProgress(neobench.ProgressReport{
				Section:      "init",
				Step:         "simulating dynamic content creation",
				Completeness: float64(actionsTaken) / float64(estTotalActions),
			})
		}

		if len(actions) > 1000 {
			if err := performActions(); err != nil {
				return err
			}
			actions = actions[:0]
		}
	}

	if len(actions) > 0 {
		if err := performActions(); err != nil {
			return err
		}
	}

	return nil
}

var ldbcNoop = statement{
	query:  "RETURN 'noop'",
	params: nil,
}

type choiceMatrix32 struct {
	entries [][]int32
	random  *rand.Rand
}

func (c *choiceMatrix32) insert(key, val int) {
	c.entries[key] = append(c.entries[key], int32(val))
}

// Exponential random distribution choice among entries for given key
func (c *choiceMatrix32) pickExponential(key int) int {
	entries := c.entries[key]
	if len(entries) == 0 {
		return 0
	}
	index, _ := neobench.ExponentialRand(c.random, 0, int64(len(entries))-1, 5.0)
	return int(entries[index])
}

func (c *choiceMatrix32) contains(key, val int) bool {
	for _, entry := range c.entries[key] {
		if entry == int32(val) {
			return true
		}
	}
	return false
}

func (c *choiceMatrix32) count(key int) int {
	return len(c.entries[key])
}

type statement struct {
	query  string
	params map[string]interface{}
}

// session.Run() does not surface errors, so emulate it
func runQ(session neo4j.Session, query string, params map[string]interface{}) error {
	_, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		res, err := tx.Run(query, params)
		if err != nil {
			return nil, err
		}
		_, err = res.Consume()
		return nil, err
	})
	return err
}

type ldbcMessageId struct {
	forumId      int
	messageIndex int
}

func (m ldbcMessageId) serialize() int64 {
	return int64(m.forumId)<<(8*4) | int64(m.messageIndex)
}

func deserializeMessageId(raw int64) ldbcMessageId {
	forumId := raw >> (8 * 4)
	messageIndex := raw & 0xffffffff
	return ldbcMessageId{
		forumId:      int(forumId),
		messageIndex: int(messageIndex),
	}
}

// List of 3-tuples; each tuple is (continent, country, city); countries are distributed exponentially
// across continents, cities exponentially across countries.
func generateLDBCPlaces(random *rand.Rand, numContinents, numCountries, numCities int64) (out [][]string) {
	countriesCreated := int64(0)
	citiesCreated := int64(0)

	// First, ensure at least one country per continent
	for i := int64(0); i < numContinents; i++ {
		countriesCreated++
		citiesCreated++
		out = append(out, []string{
			fmt.Sprintf("Continent-%d", i),
			fmt.Sprintf("Country-%d", i),
			fmt.Sprintf("City-%d", i)})
	}

	// Second, ensure at least one city per country
	for i := countriesCreated; i < numCountries; i++ {
		countriesCreated++
		citiesCreated++
		out = append(out, []string{
			randLDBCContinent(random, numContinents),
			fmt.Sprintf("Country-%d", i),
			fmt.Sprintf("City-%d", i)})
	}

	// Divide remaining cities across countries
	for i := citiesCreated; i < numCities; i++ {
		citiesCreated++
		out = append(out, []string{
			randLDBCContinent(random, numContinents),
			randLDBCCountry(random, numCountries),
			fmt.Sprintf("City-%d", i)})
	}

	return
}

// Return 2-tuples of (country, companyName)
func generateLDBCCompanies(random *rand.Rand, numCountries, numCompanies int64) (out [][]string) {
	for i := int64(0); i < numCompanies; i++ {
		out = append(out, []string{randLDBCCountry(random, numCountries), fmt.Sprintf("Company-%d", i)})
	}

	return
}

// Return 2-tuples of (city, universityName)
func generateLDBCUniversities(random *rand.Rand, numCities, numUniversities int64) (out [][]string) {
	for i := int64(0); i < numUniversities; i++ {
		out = append(out, []string{randLDBCCity(random, numCities), fmt.Sprintf("University-%d", i)})
	}

	return
}

// Return 2-tuples of (tagClass, parentTagClass); excludes tagClass 1, which should be created
// out of band and have no parent
func generateLDBCTagClasses(random *rand.Rand, numTagClasses int64) (out [][]string) {
	for i := int64(1); i < numTagClasses; i++ {
		out = append(out, []string{fmt.Sprintf("TagClass-%d", i), randLDBCTagClass(random, i)})
	}
	return
}

func createLDBCPerson(random *rand.Rand, session neo4j.Session, personNo int, creationDate time.Time, numCities, numUniversities, numCompanies, numTags int64) statement {
	birthDayOfYear, _ := neobench.ExponentialRand(random, 0, 364, 5.0)
	birthYear := ldbcStartYear - 80 + random.Intn(70)
	birthDay := time.Date(birthYear, 0, 0, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(birthDayOfYear))

	unisStudiedAt := ""
	for i := 0; i < random.Intn(4); i++ {
		uniName := randLDBCUniversity(random, numUniversities)
		classYear := birthYear + 19 + 4*i
		unisStudiedAt += fmt.Sprintf(`WITH p MATCH (uni%d:University {name: '%s'})
CREATE (p)-[:STUDY_AT {classYear: %d}]->(uni%d)
`, i, uniName, classYear, i)
	}

	companiesWorkedAt := ""
	for i := 0; i < random.Intn(8); i++ {
		compName := randLDBCCompany(random, numCompanies)
		workFrom := birthYear + 18 + i*2
		companiesWorkedAt += fmt.Sprintf(`WITH p MATCH (comp%d:Company {name: '%s'})
CREATE (p)-[:WORK_AT {workFrom: %d}]->(comp%d)
`, i, compName, workFrom, i)
	}

	interests := ""
	for i := 2; i < random.Intn(16); i++ {
		interests += fmt.Sprintf(`WITH p MATCH (tag%d:Tag {name: '%s'})
CREATE (p)-[:HAS_INTEREST]->(tag%d)
`, i, randLDBCTag(random, numTags), i)
	}

	return statement{
		query: `CREATE (p:Person {
  id: $personNo,
  creationDate: $creationDate,
  firstName: $firstName,
  lastName: $lastName,
  gender: $gender,
  birthday: $birthday,
  email: $personNo + "@persons.com",
  speaks: $speaks,
  browserUsed: $browserUsed,
  locationIP: $locationIP
})
WITH p MATCH (city:City {name: $city})
CREATE (p)-[:IS_LOCATED_IN]->(city)
` + unisStudiedAt + companiesWorkedAt + interests,
		params: map[string]interface{}{
			"personNo":     personNo,
			"creationDate": creationDate,
			"firstName":    randFirstName(random),
			"lastName":     randLastName(random),
			"gender":       randGender(random),
			"birthday":     birthDay,
			"birthyear":    birthYear,
			"speaks":       []string{"mandarin", "dutch"},
			"browserUsed":  "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
			"locationIP":   "127.0.0.1",
			"city":         randLDBCCity(random, numCities),
		},
	}
}

// Return 2-tuples of (tagname, tagclass)
func generateLDBCTags(random *rand.Rand, numTags, numTagClasses int64) (out [][]string) {
	for i := int64(1); i < numTags; i++ {
		out = append(out, []string{fmt.Sprintf("Tag-%d", i), randLDBCTagClass(random, numTagClasses)})
	}
	return
}

func randLDBCContinent(r *rand.Rand, numContinents int64) string {
	i, _ := neobench.ExponentialRand(r, 0, numContinents, 5.0)
	return fmt.Sprintf("Continent-%d", i)
}

func randLDBCCountry(r *rand.Rand, numCountries int64) string {
	i, _ := neobench.ExponentialRand(r, 0, numCountries, 5.0)
	return fmt.Sprintf("Country-%d", i)
}

func randLDBCCity(r *rand.Rand, numCities int64) string {
	i, _ := neobench.ExponentialRand(r, 0, numCities, 5.0)
	return fmt.Sprintf("City-%d", i)
}

func randLDBCTagClass(r *rand.Rand, numTagClasses int64) string {
	i, _ := neobench.ExponentialRand(r, 0, numTagClasses, 5.0)
	return fmt.Sprintf("TagClass-%d", i)
}

func randLDBCTag(r *rand.Rand, numTags int64) string {
	i, _ := neobench.ExponentialRand(r, 0, numTags, 5.0)
	return fmt.Sprintf("Tag-%d", i)
}

func randLDBCTags(r *rand.Rand, numTags int64) []string {
	nTags := r.Intn(6) + 1
	tags := make([]string, 0, nTags)
	for i := 0; i < nTags; i++ {
		tags = append(tags, randLDBCTag(r, numTags))
	}
	return tags
}

func randLDBCUniversity(r *rand.Rand, numUniversities int64) string {
	i, _ := neobench.ExponentialRand(r, 0, numUniversities, 5.0)
	return fmt.Sprintf("University-%d", i)
}

func randLDBCCompany(r *rand.Rand, numCompanies int64) string {
	i, _ := neobench.ExponentialRand(r, 0, numCompanies, 5.0)
	return fmt.Sprintf("Company-%d", i)
}

func randLDBCPersonId(r *rand.Rand, numPeople int64) int {
	i, _ := neobench.ExponentialRand(r, 1, numPeople+1, 5.0)
	return int(i)
}

func randFirstName(r *rand.Rand) string {
	i, _ := neobench.ExponentialRand(r, 0, 500, 5.0)
	return fmt.Sprintf("Jane-%d", i)
}

func randLastName(r *rand.Rand) string {
	i, _ := neobench.ExponentialRand(r, 0, 500, 5.0)
	return fmt.Sprintf("Doe-%d", i)
}

func randGender(random *rand.Rand) string {
	return fmt.Sprintf("gender-%d", random.Intn(5))
}

func randBrowser(random *rand.Rand) string {
	return "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"
}

func randLDBCMessageContent(random *rand.Rand) string {
	// Sampling an LDBC store said there's a strong bias towards very short - 2-7 characters - message contents, so
	// we do exponential spread from there; sampling 1000 entries we saw no string longer than 183 chars, which is
	// pretty weird TBH, it's like they are tweets rather than comments and posts? Making ours a bit larger.
	lorem := "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
	msgLen, _ := neobench.ExponentialRand(random, 2, int64(len(lorem)), 10.0)
	// This will give skewed results once you start looking at compression..
	return lorem[:msgLen]
}

type schemaEntry struct {
	Label    string
	Property string
	Unique   bool
}

// Note that this function has injection vulnerabilities, do not call with untrusted label or prop
// This can be deleted if we drop support for Neo4j < 4.2
func ensureSchema(session neo4j.Session, desiredSchema []schemaEntry) error {
	actualSchema, err := listSchema(session)
	if err != nil {
		return errors.Wrapf(err, "failed to list existing schema")
	}

	for _, desired := range desiredSchema {
		found := false
		for _, actual := range actualSchema {
			if actual.Label == desired.Label && actual.Property == desired.Property {
				if actual.Unique != desired.Unique {
					return fmt.Errorf("schema entry for %v already exists but uniqueness config does not match; please drop existing constraint or index, existing uniq=%v", desired, actual.Unique)
				}
				found = true
				break
			}
		}
		if found {
			continue
		}
		if desired.Unique {
			err = runQ(session, fmt.Sprintf("CREATE CONSTRAINT ON (n:%s) ASSERT n.%s IS UNIQUE", desired.Label, desired.Property), nil)
			if err != nil {
				return errors.Wrapf(err, "failed to create uniqueness constraint on (:%s).%s", desired.Label, desired.Property)
			}
		} else {
			err = runQ(session, fmt.Sprintf("CREATE INDEX FOR (p:%s) ON (p.%s)", desired.Label, desired.Property), nil)
			if err != nil {
				return errors.Wrapf(err, "failed to create index on (:%s).%s", desired.Label, desired.Property)
			}
		}
	}
	return nil
}

func listSchema(session neo4j.Session) (out []schemaEntry, err error) {
	res, err := session.Run("CALL db.indexes", nil)
	if err != nil {
		return nil, err
	}

	for res.Next() {
		rawUniqueness, _ := res.Record().Get("uniqueness")
		uniqueness := rawUniqueness.(string)
		rawLbls, _ := res.Record().Get("labelsOrTypes")
		lbls := rawLbls.([]interface{})
		rawProps, _ := res.Record().Get("properties")
		props := rawProps.([]interface{})
		if len(lbls) == 1 && len(props) == 1 {
			existingLbl := lbls[0].(string)
			existingProp := props[0].(string)
			out = append(out, schemaEntry{existingLbl, existingProp, uniqueness == "UNIQUE"})
		}
	}

	return
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
