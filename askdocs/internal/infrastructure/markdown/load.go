// Package markdown turns the lab's Markdown files into sections, one per heading.
package markdown

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"strings"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
)

// supersededDir holds runs that a later run replaced. Their numbers are kept for the record
// but are no longer the lab's answer, so a question must not be answered from them.
const supersededDir = "superseded"

var (
	ErrNoDocuments = errors.New("markdown: no Markdown files under the given roots")
	ErrRootMissing = errors.New("markdown: a root does not exist")
)

// Load reads every .md file under each root of fsys. A root is a path inside fsys: fs.FS
// refuses ".." in one, so a root cannot name a directory above fsys.
func Load(ctx context.Context, fsys fs.FS, roots ...string) ([]docs.Section, error) {
	var sections []docs.Section
	for _, root := range roots {
		found, err := loadRoot(ctx, fsys, root)
		if err != nil {
			return nil, err
		}
		sections = append(sections, found...)
	}
	if len(sections) == 0 {
		return nil, fmt.Errorf("load %v: %w", roots, ErrNoDocuments)
	}
	return sections, nil
}

func loadRoot(ctx context.Context, fsys fs.FS, root string) ([]docs.Section, error) {
	var sections []docs.Section
	err := fs.WalkDir(fsys, root, func(name string, entry fs.DirEntry, err error) error {
		switch {
		case err != nil:
			return err
		case ctx.Err() != nil:
			return ctx.Err()
		case entry.IsDir() && entry.Name() == supersededDir:
			return fs.SkipDir
		case entry.IsDir() || path.Ext(name) != ".md":
			return nil
		}
		content, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		found, err := split(name, string(content))
		sections = append(sections, found...)
		return err
	})
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, fmt.Errorf("load %s: %w", root, errors.Join(ErrRootMissing, err))
	case err != nil:
		return nil, fmt.Errorf("load %s: %w", root, err)
	}
	return sections, nil
}

// split cuts a document at its headings. Lines inside a fenced code block are code, even when
// they start with "#" (a shell comment), so they never open a section.
func split(source, content string) ([]docs.Section, error) {
	document := splitter{
		source: source,
	}
	for line := range strings.Lines(content) {
		if err := document.read(line); err != nil {
			return nil, err
		}
	}
	if err := document.flush(); err != nil {
		return nil, err
	}
	return document.sections, nil
}

type splitter struct {
	source   string
	heading  string
	body     strings.Builder
	inFence  bool
	sections []docs.Section
}

func (s *splitter) read(line string) error {
	if isFence(line) {
		s.inFence = !s.inFence
	}
	title, isHeading := headingOf(line)
	if !isHeading || s.inFence {
		s.body.WriteString(line)
		return nil
	}
	if err := s.flush(); err != nil {
		return err
	}
	s.heading = title
	return nil
}

// flush closes the section being read; a heading with nothing under it adds none.
func (s *splitter) flush() error {
	text := s.body.String()
	s.body.Reset()
	if strings.TrimSpace(text) == "" {
		return nil
	}
	section, err := docs.NewSection(s.source, s.heading, text)
	if err != nil {
		return fmt.Errorf("section %q of %s: %w", s.heading, s.source, err)
	}
	s.sections = append(s.sections, section)
	return nil
}

func isFence(line string) bool {
	trimmed := strings.TrimSpace(line)
	return strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~")
}

// headingOf reads an ATX heading ("## Title"). A run of "#" without a space after it is not
// a heading in CommonMark, so "#hashtag" stays body text.
func headingOf(line string) (string, bool) {
	trimmed := strings.TrimRight(line, "\r\n")
	level := len(trimmed) - len(strings.TrimLeft(trimmed, "#"))
	if level == 0 || level > 6 || !strings.HasPrefix(trimmed[level:], " ") {
		return "", false
	}
	return strings.TrimSpace(trimmed[level:]), true
}
