import sqlite3
import unittest
from jovanica_v7.messaging import BubbleOutbox, normalize_bubbles, typing_seconds


class FakeTelegram:
    def __init__(self, fail_send=False):
        self.actions = []
        self.fail_send = fail_send

    def __call__(self, method, **kwargs):
        self.actions.append((method, kwargs))
        if self.fail_send and method == 'sendMessage':
            raise TimeoutError('Network timeout; delivery unknown')
        if method == 'sendMessage':
            return {'message_id': 100 + sum(x[0] == 'sendMessage' for x in self.actions)}
        return True


class MessagingTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.outbox = BubbleOutbox(self.db)
        self.api = FakeTelegram()
        self.ok = lambda cid, chat: True

    def test_parse_legacy(self):
        self.assertEqual(normalize_bubbles({'reply': '  ćao   '}), ['ćao'])

    def test_parse_multi(self):
        self.assertEqual(normalize_bubbles({'messages': [' ćao ', ' šta ima?']}), ['ćao', 'šta ima?'])

    def test_reject_invalid(self):
        for candidate in [[], [''], ['a', 'b', 'c', 'd'], ['x' * 3901], {'reply': None}]:
            with self.assertRaises(ValueError):
                normalize_bubbles(candidate)

    def test_typing_limits(self):
        self.assertGreaterEqual(typing_seconds('x'), 0.7)
        self.assertLessEqual(typing_seconds('x' * 2000), 4.0)

    def test_order_and_typing(self):
        self.outbox.enqueue('bc', '123', 'job-1', {'messages': ['ćao', 'kako si?', 'šta radiš?']}, now=100)
        events = []
        for step in range(20):
            event = self.outbox.tick(self.api, allowed=self.ok, now=100+step*5)
            if event:
                events.append(event['event'])
        self.assertEqual(events, ['typing', 'sent', 'typing', 'sent', 'typing', 'sent'])
        self.assertEqual([x[0] for x in self.api.actions],
                         ['sendChatAction', 'sendMessage']*3)
        self.assertEqual([v[-1] for v in self.outbox.snapshot('bc','123')], ['sent']*3)

    def test_new_incoming_interrupts_unsent(self):
        self.outbox.enqueue('bc', '123', 'old', ['one', 'two'], now=100)
        self.outbox.tick(self.api, allowed=self.ok, now=100)
        self.outbox.interrupt('bc','123')
        self.assertIsNone(self.outbox.tick(self.api, allowed=self.ok, now=200))
        self.assertFalse([m for m,k in self.api.actions if m=='sendMessage'])
        self.assertTrue(all(status=='cancelled' for _,_,status in self.outbox.snapshot('bc','123')))

    def test_new_reply_replaces_old(self):
        self.outbox.enqueue('bc','123','old',['one','two'],now=100)
        self.outbox.enqueue('bc','123','new',['three'],now=101)
        e = self.outbox.tick(self.api,allowed=self.ok,now=102)
        self.assertEqual(e['event'],'typing')
        self.outbox.tick(self.api,allowed=self.ok,now=110)
        messages = [kwargs['text'] for method,kwargs in self.api.actions if method=='sendMessage']
        self.assertEqual(messages,['three'])

    def test_duplicate_job_is_idempotent(self):
        first=self.outbox.enqueue('bc','123','same',['one'],now=100)
        second=self.outbox.enqueue('bc','123','same',['DIFFERENT'],now=101)
        self.assertEqual(first,second)
        self.assertEqual(len(self.outbox.snapshot('bc','123')),1)

    def test_paused_chat_never_sends(self):
        self.outbox.enqueue('bc','123','job',['hello'],now=100)
        self.assertEqual(self.outbox.tick(self.api,allowed=lambda *_:False,now=100)['event'],'cancelled')
        self.assertEqual(self.api.actions,[])

    def test_uncertain_send_not_retried(self):
        self.api = FakeTelegram(fail_send=True)
        self.outbox.enqueue('bc','123','job',['one','two'],now=100)
        self.outbox.tick(self.api,allowed=self.ok,now=100)
        self.assertEqual(self.outbox.tick(self.api,allowed=self.ok,now=110)['event'],'uncertain')
        self.assertIsNone(self.outbox.tick(self.api,allowed=self.ok,now=200))
        self.assertEqual(sum(x[0]=='sendMessage' for x in self.api.actions),1)
        self.assertEqual([x[-1] for x in self.outbox.snapshot('bc','123')],['uncertain','cancelled'])

    def test_recover_sending_marks_uncertain(self):
        self.outbox.enqueue('bc','123','job',['one','two'],now=100)
        self.db.execute("UPDATE v7_bubble_outbox SET status='sending' WHERE position=0")
        self.outbox.recover()
        self.assertEqual([x[-1] for x in self.outbox.snapshot('bc','123')],['uncertain','cancelled'])

    def test_recover_typing_restarts_safely(self):
        self.outbox.enqueue('bc','123','job',['one'],now=100)
        self.outbox.tick(self.api,allowed=self.ok,now=100)
        self.outbox.recover()
        self.assertEqual(self.outbox.snapshot('bc','123')[0][-1],'pending')


if __name__ == '__main__':
    unittest.main()
