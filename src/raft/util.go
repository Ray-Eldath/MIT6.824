package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type logTopic string

const (
	dLog       logTopic = "LOG"  // AppendEntries
	dVote      logTopic = "VOTE" // RequestVote
	dElection  logTopic = "ELCT"
	dHeartbeat logTopic = "HRBT"
	dClient    logTopic = "CLNT"
	dCommit    logTopic = "CMIT"
	dLeader    logTopic = "LEAD"
	dPersist   logTopic = "PERS"
	dSnapshot  logTopic = "SNAP"

	dTrace logTopic = "TRACE"
	dInfo  logTopic = "INFO"
	dWarn  logTopic = "WARN"
	dError logTopic = "ERROR"
	dFatal logTopic = "FATAL"
)

var debugStart time.Time
var debugVerbosity int

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func (rf *Raft) FormatLog() string {
	if debugVerbosity > 1 {
		s := ""
		for i := 1; i < len(rf.log); i++ {
			s += fmt.Sprintf("%v ", rf.log[i])
		}
		return s
	} else {
		return "require getVerbosity() > 1"
	}
}

func (rf *Raft) FormatFullLog() string {
	if debugVerbosity > 1 {
		return fmt.Sprintf("%s %s", rf.log[0], rf.FormatLog())
	} else {
		return "require getVerbosity() > 1"
	}
}

func (rf *Raft) FormatState() string {
	return fmt.Sprintf("%s  full log: %v", rf.FormatStateOnly(), rf.log)
}

func (rf *Raft) FormatStateOnly() string {
	return fmt.Sprintf("commitIndex=%d lastApplied=%d nextIndex=%v matchIndex=%v", rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
}

const Padding = "    "

func (rf *Raft) Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity > 0 {
		log.Print(rf.Sdebug(topic, format, a...))
	}
}

func (rf *Raft) Sdebug(topic logTopic, format string, a ...interface{}) string {
	preamble := strings.Repeat(Padding, rf.me)
	epilogue := strings.Repeat(Padding, len(rf.peers)-rf.me-1)
	prefix := fmt.Sprintf("%s%s %-5s [%s t%02d S%d] %s", preamble, Microseconds(time.Now()), string(topic), rf.state, rf.term, rf.me, epilogue)
	format = prefix + format
	return fmt.Sprintf(format, a...)
}

func Microseconds(t time.Time) string {
	return fmt.Sprintf("%06d", t.Sub(debugStart).Microseconds()/100)
}
