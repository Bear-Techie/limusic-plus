// Query the bot's accessible voice channels without creating any voice connection.
const token = process.env.LIMUSIC_DISCORD_BOT_TOKEN;
delete process.env.LIMUSIC_DISCORD_BOT_TOKEN;
function reply(result) {
	process.stdout.write(JSON.stringify(result), () => process.exit(0));
}
const deadline = setTimeout(() => reply({ error: 'connection' }), 20_000);

async function discover() {
	const { Client, Events, GatewayIntentBits, ChannelType, PermissionFlagsBits } = await import('discord.js');
	const client = new Client({ intents: [GatewayIntentBits.Guilds] });
	client.on('error', () => reply({ error: 'connection' }));
	const ready = new Promise(resolve => client.once(Events.ClientReady, resolve));
	try {
		await client.login(token);
		await ready;
		const permissions = [
			PermissionFlagsBits.ViewChannel, PermissionFlagsBits.Connect, PermissionFlagsBits.Speak
		];
		const channels = [];
		for (const guild of client.guilds.cache.values()) {
			if (!guild.available) continue;
			const member = guild.members.me ?? await guild.members.fetchMe().catch(() => null);
			if (!member) continue;
			for (const channel of guild.channels.cache.values()) {
				if (channel.type === ChannelType.GuildVoice && channel.permissionsFor(member)?.has(permissions)) {
					channels.push({ id: channel.id, name: channel.name, guild: guild.name });
				}
			}
		}
		channels.sort((a, b) => a.guild.localeCompare(b.guild) || a.name.localeCompare(b.name));
		return { id: client.application.id, name: client.user.username, channels };
	} catch (error) {
		return { error: [4004, 401, 'TokenInvalid'].includes(error?.code) || error?.status === 401 ? 'token' : 'connection' };
	} finally {
		await client.destroy();
	}
}

const result = token ? await discover().catch(() => ({ error: 'dependencies' })) : { error: 'token' };
clearTimeout(deadline);
reply(result);
