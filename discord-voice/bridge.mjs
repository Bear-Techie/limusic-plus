// Limusic launches this module with the dependency directory as its working directory.
// stdin carries live PCM and stdout carries status codes. Keep credentials out of both.
const token = process.env.LIMUSIC_DISCORD_BOT_TOKEN;
const channelId = process.env.LIMUSIC_DISCORD_CHANNEL;
delete process.env.LIMUSIC_DISCORD_BOT_TOKEN;

let client;
let connection;
let player;
let ending = false;
async function finish(status) {
	if (ending) return;
	ending = true;
	player?.stop(true);
	try {
		if (connection && connection.state.status !== 'destroyed') {
			// Wait briefly for Discord to acknowledge the leave before closing the gateway.
			await new Promise((resolve) => {
				const timeout = setTimeout(done, 750);
				function done() {
					clearTimeout(timeout);
					client.off('voiceStateUpdate', onVoiceState);
					resolve();
				}
				function onVoiceState(_, state) {
					if (state.id === client.user?.id && state.guild.id === connection.joinConfig.guildId &&
						state.channelId === null) done();
				}
				client.on('voiceStateUpdate', onVoiceState);
				connection.destroy();
			});
		}
		await client?.destroy();
	} catch {
		// The gateway may already have closed.
	}
	if (status) process.stdout.write(status + '\n', () => process.exit(1));
	else process.exit(0);
}

process.stdin.once('end', () => finish());
process.stdin.on('error', () => finish('audio'));
process.stdin.resume();
process.on('uncaughtException', () => finish('connection'));
process.on('unhandledRejection', () => finish('connection'));

const packages = await Promise.all([
	import('discord.js'),
	import('@discordjs/voice'),
	import('opusscript')
]).catch(() => finish('dependencies'));

async function connect([discord, voice]) {
	if (!token) {
		await finish('token');
		return;
	}
	client = new discord.Client({
		intents: [discord.GatewayIntentBits.Guilds, discord.GatewayIntentBits.GuildVoiceStates]
	});
	client.on('error', () => finish('connection'));
	const ready = new Promise(resolve => client.once(discord.Events.ClientReady, resolve));
	try {
		await client.login(token);
		await ready;
	} catch (error) {
		finish(error?.code === 'TokenInvalid' || error?.code === 4004 ? 'token' : 'connection');
		return;
	}
	const channel = await client.channels.fetch(channelId).catch(() => null);
	if (channel?.type !== discord.ChannelType.GuildVoice ||
		!channel.permissionsFor(client.user)?.has(['ViewChannel', 'Connect', 'Speak'])) {
		finish('channel');
		return;
	}
	if (channel.guild.voiceStates.cache.get(client.user.id)?.channelId) {
		finish('occupied');
		return;
	}

	connection = voice.joinVoiceChannel({
		channelId,
		guildId: channel.guild.id,
		adapterCreator: channel.guild.voiceAdapterCreator,
		selfDeaf: true,
		selfMute: false
	});
	connection.on('error', () => finish('connection'));
	connection.once(voice.VoiceConnectionStatus.Destroyed, () => finish('connection'));
	await voice.entersState(connection, voice.VoiceConnectionStatus.Ready, 20_000);

	process.stdin.pause();
	player = voice.createAudioPlayer({
		behaviors: { maxMissedFrames: Infinity, noSubscriber: voice.NoSubscriberBehavior.Play }
	});
	player.on('error', () => finish('audio'));
	const resource = voice.createAudioResource(process.stdin, { inputType: voice.StreamType.Raw });
	if (!connection.subscribe(player)) {
		finish('connection');
		return;
	}
	player.play(resource);
	process.stdout.write('ready\n');

	let reconnectTimer;
	connection.on('stateChange', (previous, next) => {
		if (ending) return;
		if (next.status === voice.VoiceConnectionStatus.Ready) {
			clearTimeout(reconnectTimer);
			reconnectTimer = undefined;
			process.stdout.write('ready\n');
		} else if (previous.status === voice.VoiceConnectionStatus.Ready) {
			process.stdout.write('reconnecting\n');
			reconnectTimer = setTimeout(() => finish('connection'), 15_000);
		}
	});
}

if (packages) await connect(packages).catch(() => finish('connection'));
