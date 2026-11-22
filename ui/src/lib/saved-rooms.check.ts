import { checkFriendRooms, savedRoomKey, sortedSavedRooms } from './saved-rooms.ts';

function equal(actual: unknown, expected: unknown, message = 'Unexpected room order') {
	if (JSON.stringify(actual) !== JSON.stringify(expected)) throw new Error(message);
}
const assert = { deepEqual: equal, equal, notEqual: (a: unknown, b: unknown) => {
	if (a === b) throw new Error('Room keys must distinguish different servers and codes');
} };

const room = (roomCode: string, lastUsedMs: number, owner = false, favorite = false) => ({
	roomCode, lastUsedMs, owner, favorite, serverUrl: 'limusic-peer://friend', username: 'me', hostName: roomCode, name: ''
});
const recentClosed = room('recent-closed', 900, false, true);
const olderOpen = room('older-open', 10);
const recentOpen = room('recent-open', 100);
const own = room('own', 1000, true);
const rooms = [recentClosed, olderOpen, own, recentOpen];
const availability = {
	[savedRoomKey(recentClosed)]: 'closed' as const,
	[savedRoomKey(olderOpen)]: 'open' as const,
	[savedRoomKey(recentOpen)]: 'open' as const
};
assert.deepEqual(sortedSavedRooms(rooms, 'join', availability).map(r => r.roomCode), ['recent-open', 'older-open', 'recent-closed']);
assert.deepEqual(sortedSavedRooms(rooms, 'host', availability), [own]);
assert.deepEqual(sortedSavedRooms(rooms, 'join', {}).map(r => r.roomCode), ['recent-closed', 'recent-open', 'older-open']);
const favorite = room('z', 1, false, true);
assert.equal(sortedSavedRooms([room('a', 1), favorite], 'join', {})[0], favorite);
assert.deepEqual(rooms, [recentClosed, olderOpen, own, recentOpen], 'sorting must not mutate persisted order');
assert.notEqual(savedRoomKey({...own, serverUrl:'a|b', roomCode:'c'}), savedRoomKey({...own, serverUrl:'a', roomCode:'b|c'}));

let running = 0, maximum = 0, calls = 0;
const batch = await checkFriendRooms([own, ...Array.from({length:8}, (_, i) => room(String(i), i))], async (r) => {
	calls++; running++; maximum = Math.max(maximum, running);
	await Promise.resolve();
	running--;
	if (r.roomCode === '2') throw new Error('connection unavailable');
	return r.roomCode === '0' ? 'open' : 'closed';
});
equal(calls, 8, 'startup and Join checks must skip owned rooms');
equal(maximum, 4, 'limit simultaneous checks');
equal(batch[savedRoomKey(room('0', 0))], 'open');
equal(batch[savedRoomKey(room('2', 2))], 'unknown', 'a network failure must not report closed');
let active = true, lateCallbacks = 0;
const cancelled = await checkFriendRooms(rooms, async () => { active = false; return 'open'; }, () => { lateCallbacks++; }, () => active);
equal(cancelled, {});
equal(lateCallbacks, 0, 'closing a list or dismissing startup discovery must suppress late results');
console.log('ok');
