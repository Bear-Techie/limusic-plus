import { makeRoomLink, parseRoomLink } from './room-link.ts';

function check(condition: boolean, message: string) {
	if (!condition) throw new Error(message);
}

const peer = 'ac'.repeat(32);
const server = `limusic-peer://${peer}`;
const code = 'ROOM1234';
const link = `limusic-plus://join/${peer}/${code}`;

check(makeRoomLink(server, code.toLowerCase()) === link, 'copy emits a canonical room invite');
const parsed = parseRoomLink(link);
check(parsed?.server === server && parsed.code === code, 'open reaches the original room');
check(makeRoomLink('wss://example.com/ws', code) === null, 'only peer-hosted rooms generate app links');
check(makeRoomLink(server, '../bad') === null, 'reject path traversal');
for (const invalid of [
	`${link}?server=wss://example.com`,
	`${link}#anything`,
	`${link}/other`,
	link.replace('/join/', '/host/'),
	link.replace(peer, 'example.com')
]) {
	check(parseRoomLink(invalid) === null, `reject unexpected app link: ${invalid}`);
}
console.log('ok — room invites resolve only to a peer endpoint and room code');
