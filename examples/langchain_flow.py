"""
Blog Post Generator Pipeline using LangChain

This example demonstrates a sequential Symphony pipeline that orchestrates
LangChain agents to research a topic, scrape content, and generate a blog post.

pip install langchain langchain-openai langchain-community duckduckgo-search newspaper3k symphony-orc
"""

import json
import asyncio
from textwrap import dedent
from typing import Dict, List, Optional, Any

from symphony import Pipeline, create_step
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from langchain_community.tools import DuckDuckGoSearchResults
from pydantic import BaseModel, Field
from rich.prompt import Prompt
from rich.console import Console

console = Console()

# --- Pydantic Schemas ---

class NewsArticle(BaseModel):
    """A single news article from search results."""
    title: str = Field(..., description="Title of the article")
    url: str = Field(..., description="URL of the article")
    snippet: str = Field(..., description="Brief snippet or summary")


class SearchResults(BaseModel):
    """Collection of articles found during research."""
    topic: str = Field(..., description="The original search topic")
    articles: List[NewsArticle] = Field(default_factory=list, description="List of found articles")


class ScrapedArticle(BaseModel):
    """An article with its full extracted content."""
    title: str = Field(..., description="Title of the article")
    url: str = Field(..., description="URL of the article")
    content: str = Field(..., description="Full extracted content in markdown")


class ScrapedArticlesOutput(BaseModel):
    """Collection of scraped articles."""
    topic: str = Field(..., description="The original topic")
    articles: List[ScrapedArticle] = Field(default_factory=list)


class BlogPostOutput(BaseModel):
    """The final generated blog post."""
    title: str = Field(..., description="Blog post title")
    content: str = Field(..., description="Full blog post content in markdown")


class SearchStepInput(BaseModel):
    """Input for the search step."""
    topic: str = Field(..., description="Topic to research")


# --- LangChain Setup ---

# Models
search_model = ChatOpenAI(model="gpt-4o-mini", temperature=0)
writer_model = ChatOpenAI(model="gpt-4o", temperature=0.7)

# Tools
search_tool = DuckDuckGoSearchResults(num_results=5)

# Parsers
search_parser = PydanticOutputParser(pydantic_object=SearchResults)
blog_parser = PydanticOutputParser(pydantic_object=BlogPostOutput)

# --- Prompts ---

SEARCH_ANALYSIS_PROMPT = ChatPromptTemplate.from_messages([
    ("system", dedent("""\
        You are BlogResearch-X, an elite research assistant specializing in 
        discovering high-quality sources for compelling blog content.
        
        Your job is to analyze raw search results and extract the most relevant,
        authoritative articles for blog research.
        
        {format_instructions}
    """)),
    ("human", dedent("""\
        Topic: {topic}
        
        Raw search results:
        {search_results}
        
        Analyze these results and return the 3-5 most relevant, authoritative articles.
        Prioritize recent content with unique angles and expert insights.
    """))
])

CONTENT_EXTRACTION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", dedent("""\
        You are ContentBot-X, a specialist in processing and summarizing article content.
        
        Given an article's snippet and URL, create a comprehensive content summary that 
        captures key points, quotes, statistics, and insights that would be valuable 
        for blog creation.
        
        Format the content in clean markdown.
    """)),
    ("human", dedent("""\
        Article Title: {title}
        Article URL: {url}
        Article Snippet: {snippet}
        
        Generate a detailed content summary (300-500 words) based on the snippet,
        expanding on the key themes and insights. Structure it with relevant subheadings.
    """))
])

BLOG_WRITER_PROMPT = ChatPromptTemplate.from_messages([
    ("system", dedent("""\
        You are BlogMaster-X, an elite content creator combining journalistic excellence
        with digital marketing expertise.
        
        Write engaging, well-researched blog posts that:
        - Have viral-worthy headlines
        - Include compelling introductions
        - Structure content for digital consumption
        - Incorporate research seamlessly
        - Optimize for SEO while maintaining quality
        
        {format_instructions}
    """)),
    ("human", dedent("""\
        Topic: {topic}
        
        Research Content:
        {research_content}
        
        Write a comprehensive, engaging blog post based on this research.
        Include proper source attribution and key takeaways.
        Target length: 800-1200 words.
    """))
])


# --- Step Handler Functions ---

def search_for_articles(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """
    Step 1: Search for articles on the given topic using DuckDuckGo.
    Uses LangChain to analyze and structure the results.
    """
    topic = params["payload"]["topic"]
    console.print(f"[bold blue]🔍 Searching for articles on:[/bold blue] {topic}")
    
    # Execute search
    raw_results = search_tool.invoke(topic)
    console.print(f"[dim]Found raw results, analyzing...[/dim]")
    
    # Use LLM to analyze and structure results
    chain = SEARCH_ANALYSIS_PROMPT | search_model | search_parser
    
    result = chain.invoke({
        "topic": topic,
        "search_results": raw_results,
        "format_instructions": search_parser.get_format_instructions()
    })
    
    console.print(f"[green]✓ Found {len(result.articles)} relevant articles[/green]")
    for article in result.articles:
        console.print(f"  [dim]• {article.title[:60]}...[/dim]")
    
    return result.model_dump()


def scrape_and_process_articles(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """
    Step 2: Process each article and extract detailed content summaries.
    Uses LangChain to expand snippets into full content.
    """
    search_results = params["payload"]
    topic = search_results["topic"]
    articles = search_results["articles"]
    
    console.print(f"\n[bold blue]📰 Processing {len(articles)} articles...[/bold blue]")
    
    scraped_articles = []
    chain = CONTENT_EXTRACTION_PROMPT | search_model
    
    for article in articles:
        console.print(f"  [dim]Extracting: {article['title'][:50]}...[/dim]")
        
        response = chain.invoke({
            "title": article["title"],
            "url": article["url"],
            "snippet": article["snippet"]
        })
        
        scraped_articles.append({
            "title": article["title"],
            "url": article["url"],
            "content": response.content
        })
    
    console.print(f"[green]✓ Processed {len(scraped_articles)} articles[/green]")
    
    return {
        "topic": topic,
        "articles": scraped_articles
    }


def generate_blog_post(params: Dict[str, Any], perf) -> Dict[str, Any]:
    """
    Step 3: Generate the final blog post from processed research.
    Uses GPT-4 for high-quality content generation.
    """
    data = params["payload"]
    topic = data["topic"]
    articles = data["articles"]
    
    console.print(f"\n[bold blue]✍️  Generating blog post...[/bold blue]")
    
    # Format research content
    research_content = "\n\n---\n\n".join([
        f"### {a['title']}\nSource: {a['url']}\n\n{a['content']}"
        for a in articles
    ])
    
    chain = BLOG_WRITER_PROMPT | writer_model | blog_parser
    
    result = chain.invoke({
        "topic": topic,
        "research_content": research_content,
        "format_instructions": blog_parser.get_format_instructions()
    })
    
    console.print(f"[green]✓ Blog post generated: {result.title}[/green]")
    
    return result.model_dump()


# --- Create Symphony Steps ---

search_step = create_step(
    id="langchain_search",
    description="Search for articles using DuckDuckGo and analyze with LangChain",
    handler=search_for_articles,
    input_schema=SearchStepInput,
    output_schema=SearchResults
)

scrape_step = create_step(
    id="langchain_scrape",
    description="Process and extract content from found articles",
    handler=scrape_and_process_articles,
    input_schema=SearchResults,
    output_schema=ScrapedArticlesOutput
)

write_step = create_step(
    id="langchain_write",
    description="Generate final blog post from research",
    handler=generate_blog_post,
    input_schema=ScrapedArticlesOutput,
    output_schema=BlogPostOutput
)


# --- Create the Symphony Pipeline ---

blog_pipeline = (
    Pipeline(
        id="langchain_blog_pipeline",
        description="Sequential pipeline to generate blog posts using LangChain agents"
    )
    .pipe(search_step)
    .pipe(scrape_step)
    .pipe(write_step)
    .seal()
)


# --- Main Execution ---

async def main():
    console.print("\n[bold]🎼 Symphony + LangChain Blog Generator[/bold]\n")
    
    topic = Prompt.ask(
        "[bold]Enter a blog post topic[/bold]",
        default="The future of AI agents in 2025"
    )
    
    console.print("\n" + "=" * 80)
    console.print(f"[bold]Running Blog Generation Pipeline for:[/bold] '{topic}'")
    console.print("=" * 80 + "\n")
    
    try:
        result = await blog_pipeline.perform({"topic": topic})
        
        console.print("\n" + "=" * 80)
        console.print("[bold green]✓ Blog Post Generation Complete![/bold green]")
        console.print("=" * 80 + "\n")
        
        console.print(f"[bold]{result['title']}[/bold]\n")
        console.print(result["content"])
        
    except Exception as e:
        console.print(f"[bold red]Pipeline failed:[/bold red] {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
