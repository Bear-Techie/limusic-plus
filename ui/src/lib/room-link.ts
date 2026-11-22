const PEER_INVITE = /^limusic-plus:\/\/join\/([0-9a-f]{64})\/([a-z0-9]{6,32})$/i;
const PEER_SERVER = /^limusic-peer:\/\/([0-9a-f]{64})$/i;

export function makeRoomLink(server: string, code: string): string | null {
	const peer = PEER_SERVER.exec(server)?.[1];
	if (!peer || !/^[a-z0-9]{6,32}$/i.test(code)) return null;
	return `limusic-plus://join/${peer.toLowerCase()}/${code.toUpperCase()}`;
}

export function parseRoomLink(input: string): { server: string; code: string } | null {
	const match = PEER_INVITE.exec(input.trim());
	return match ? { server: `limusic-peer://${match[1].toLowerCase()}`, code: match[2].toUpperCase() } : null;
}
