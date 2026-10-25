// Package ask answers a question from the lab's documentation: gather the evidence, and only
// then pay a model to read it.
package ask

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/DmytroLysenko1/Kafka-lab/askdocs/internal/domain/docs"
)

type retriever interface {
	Relevant(ctx context.Context, question docs.Question, limit int) ([]docs.Section, error)
}

type answerer interface {
	Answer(ctx context.Context, question docs.Question, evidence docs.Evidence) (docs.Answer, error)
}

type Ask struct {
	sections retriever
	model    answerer
	tracer   trace.Tracer
}

func New(sections retriever, model answerer, tracer trace.Tracer) *Ask {
	return &Ask{
		sections: sections,
		model:    model,
		tracer:   tracer,
	}
}

func (a *Ask) Execute(ctx context.Context, question docs.Question) (docs.Answer, error) {
	ctx, span := a.tracer.Start(ctx, "ask.Execute")
	defer span.End()
	answer, err := a.execute(ctx, question)
	record(span, err)
	return answer, err
}

// Evidence is the retrieval half alone: what the question would be answered from, at no cost.
func (a *Ask) Evidence(ctx context.Context, question docs.Question) (docs.Evidence, error) {
	ctx, span := a.tracer.Start(ctx, "ask.Evidence")
	defer span.End()
	evidence, err := a.evidence(ctx, question)
	record(span, err)
	return evidence, err
}

func (a *Ask) execute(ctx context.Context, question docs.Question) (docs.Answer, error) {
	evidence, err := a.evidence(ctx, question)
	if err != nil {
		return docs.Answer{}, err
	}
	return a.model.Answer(ctx, question, evidence)
}

func (a *Ask) evidence(ctx context.Context, question docs.Question) (docs.Evidence, error) {
	ranked, err := a.sections.Relevant(ctx, question, docs.MaxEvidence)
	if err != nil {
		return docs.Evidence{}, fmt.Errorf("retrieve sections for %q: %w", question, err)
	}
	return docs.NewEvidence(ranked)
}

func record(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
