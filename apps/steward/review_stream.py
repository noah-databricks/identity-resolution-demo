"""Stream only the public message field, never tool arguments or reasoning."""
import asyncio
import time
from pydantic_core import from_json
from agents import Runner


def public_text(partial):
    try:
        value = from_json(partial, allow_partial='trailing-strings')
    except ValueError:
        return ''
    text = value.get('message', '') if isinstance(value, dict) else ''
    return text if isinstance(text, str) else ''


async def run_streamed_review(agent, message, *, emit, timeout):
    result = Runner.run_streamed(agent, message, max_turns=8)
    partial, last_text, last_emit = '', '', 0.0
    try:
        async with asyncio.timeout(timeout):
            async for event in result.stream_events():
                if event.type != 'raw_response_event':
                    continue
                if event.data.type == 'response.output_text.delta':
                    partial += event.data.delta
                    text = public_text(partial)
                    if text != last_text and time.monotonic() - last_emit >= .08:
                        await emit('text_snapshot', {'text': text})
                        last_text, last_emit = text, time.monotonic()
                elif event.data.type == 'response.completed':
                    text = public_text(partial)
                    if text and text != last_text:
                        await emit('text_snapshot', {'text': text})
                        last_text = text
                    partial = ''
            text = public_text(partial)
            if text and text != last_text:
                await emit('text_snapshot', {'text': text})
        return result
    except BaseException:
        result.cancel()
        raise
